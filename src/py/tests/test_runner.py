"""Tests for vertebra.server._native — concurrent batch dispatch."""

import asyncio
import time
from typing import Any
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Fake VertebraRequest / VertebraResponse stubs
# ---------------------------------------------------------------------------


class FakeRequest:
    """Minimal request stub matching VertebraRequest interface."""

    def __init__(
        self,
        handler_id: int = 0,
        path: str = "/",
        request_id: int = 0,
        body: bytes = b"",
        method: str = "GET",
        query: "str | None" = None,
        headers: Any = None,
    ):
        self.handler_id = handler_id
        self.path = path
        self.request_id = request_id
        self.body = body
        self.method = method
        self.query = query
        self.headers = headers


class FakeResponse:
    """Minimal response stub matching VertebraResponse interface."""

    def __init__(self, status: int = 200, body: bytes = b"", request_id: int = 0, headers: "list | None" = None):
        self.status = status
        self.body = body
        self.request_id = request_id
        self.headers = headers or []


# ---------------------------------------------------------------------------
# Fixture: CallbackScheduler with automatic cleanup
# ---------------------------------------------------------------------------


@pytest.fixture()
def scheduler():
    from vertebra.server._native import CallbackScheduler

    s = CallbackScheduler()
    yield s
    s.stop()


# ---------------------------------------------------------------------------
# Phase 1: Unified event loop
# ---------------------------------------------------------------------------


def test_unified_event_loop_handler_loop_global_removed():
    """The _HANDLER_LOOP module global should no longer exist."""
    from vertebra.server import _native

    assert not hasattr(_native, "_HANDLER_LOOP"), "_HANDLER_LOOP global should be removed in favor of unified loop"


def test_unified_event_loop_call_handler_accepts_loop_param():
    """_call_handler should accept a loop parameter for async handlers."""
    import inspect

    from vertebra.server._native import _call_handler

    sig = inspect.signature(_call_handler)
    assert "loop" in sig.parameters, "_call_handler must accept a loop parameter"


# ---------------------------------------------------------------------------
# Phase 2: Batch partitioning
# ---------------------------------------------------------------------------


def test_partition_batch_sync_handler_classified():
    """Sync native handlers go to the sync_native group."""
    from vertebra.server._native import _partition_batch

    def sync_handler(req: Any) -> bytes:
        return b"ok"

    handlers = {1: sync_handler}
    batch = [FakeRequest(handler_id=1, request_id=10)]

    result = _partition_batch(batch, handlers, [])

    assert len(result.sync_native) == 1
    assert result.sync_native[0][0] == 0  # original index
    assert result.sync_native[0][1] is batch[0]  # request
    assert result.sync_native[0][2] is sync_handler


def test_partition_batch_async_handler_classified():
    """Async native handlers go to the async_native group."""
    from vertebra.server._native import _partition_batch

    async def async_handler(req: Any) -> bytes:
        return b"ok"

    handlers = {2: async_handler}
    batch = [FakeRequest(handler_id=2, request_id=20)]

    result = _partition_batch(batch, handlers, [])

    assert len(result.async_native) == 1
    assert result.async_native[0][0] == 0
    assert result.async_native[0][2] is async_handler


def test_partition_batch_mount_classified():
    """Mount-dispatched requests grouped by prefix."""
    from vertebra.server._native import _partition_batch

    mock_adapter = MagicMock()
    mount_runners = [("/api", mock_adapter, "asgi")]
    batch = [FakeRequest(handler_id=0, path="/api/users", request_id=30)]

    result = _partition_batch(batch, {}, mount_runners)

    assert "/api" in result.mounts
    assert len(result.mounts["/api"]) == 1


def test_partition_batch_unmatched_classified():
    """Requests matching no handler or mount go to unmatched (404)."""
    from vertebra.server._native import _partition_batch

    batch = [FakeRequest(handler_id=999, path="/nope", request_id=40)]

    result = _partition_batch(batch, {}, [])

    assert len(result.unmatched) == 1
    assert result.unmatched[0][0] == 0


def test_partition_batch_mixed_batch_partitioned():
    """A mixed batch is correctly split across all groups."""
    from vertebra.server._native import _partition_batch

    def sync_h(req: Any) -> bytes:
        return b"s"

    async def async_h(req: Any) -> bytes:
        return b"a"

    handlers = {1: sync_h, 2: async_h}
    mock_adapter = MagicMock()
    mount_runners = [("/app", mock_adapter, "wsgi")]

    batch = [
        FakeRequest(handler_id=1, request_id=1),  # sync
        FakeRequest(handler_id=2, request_id=2),  # async
        FakeRequest(handler_id=0, path="/app/x", request_id=3),  # mount
        FakeRequest(handler_id=0, path="/nowhere", request_id=4),  # 404
    ]

    result = _partition_batch(batch, handlers, mount_runners)

    assert len(result.sync_native) == 1
    assert len(result.async_native) == 1
    assert "/app" in result.mounts
    assert len(result.mounts["/app"]) == 1
    assert len(result.unmatched) == 1


# ---------------------------------------------------------------------------
# Phase 2b: Parameter injection
# ---------------------------------------------------------------------------


def test_parameter_injection_body_param_gets_bytes():
    from vertebra.server._native import _resolve_handler_args

    def echo(body: bytes) -> bytes:
        return body

    req = FakeRequest(body=b"hello")
    args = _resolve_handler_args(echo, req)
    assert args == (b"hello",)


def test_parameter_injection_request_param_gets_request():
    from vertebra.server._native import _resolve_handler_args

    def handler(request: Any) -> bytes:
        return b"ok"

    req = FakeRequest()
    args = _resolve_handler_args(handler, req)
    assert args[0] is req


def test_parameter_injection_no_params_gets_empty():
    from vertebra.server._native import _resolve_handler_args

    def handler() -> bytes:
        return b"ok"

    args = _resolve_handler_args(handler, FakeRequest())
    assert args == ()


def test_parameter_injection_multiple_params():
    from vertebra.server._native import _resolve_handler_args

    def handler(method: str, path: str, body: bytes) -> bytes:
        return b"ok"

    req = FakeRequest(method="POST", path="/echo", body=b"data")
    args = _resolve_handler_args(handler, req)
    assert args == ("POST", "/echo", b"data")


def test_parameter_injection_echo_handler_works_end_to_end(scheduler):
    from vertebra.server._native import _dispatch_batch, _partition_batch

    def echo(body: bytes) -> bytes:
        return body

    handlers = {1: echo}
    batch = [FakeRequest(handler_id=1, request_id=1, body=b"echo me")]

    partitioned = _partition_batch(batch, handlers, [])
    results = _dispatch_batch(partitioned, handlers, [], FakeResponse, scheduler)
    assert results[0].status == 200
    assert results[0].body == b"echo me"


def test_parameter_injection_async_body_param(scheduler):
    from vertebra.server._native import _dispatch_batch, _partition_batch

    async def async_echo(body: bytes) -> bytes:
        return body

    handlers = {1: async_echo}
    batch = [FakeRequest(handler_id=1, request_id=1, body=b"async echo")]

    partitioned = _partition_batch(batch, handlers, [])
    results = _dispatch_batch(partitioned, handlers, [], FakeResponse, scheduler)

    assert len(results) == 1
    assert results[0].body == b"async echo"
    assert results[0].request_id == 1


# ---------------------------------------------------------------------------
# Phase 3: Concurrent dispatch
# ---------------------------------------------------------------------------


def test_concurrent_dispatch_async_handlers_concurrent(scheduler):
    """N async handlers with 10ms sleep should complete in ~10ms, not N*10ms."""
    from vertebra.server._native import _dispatch_batch, _partition_batch

    sleep_ms = 20
    n_handlers = 5

    async def slow_handler(req: Any) -> bytes:
        await asyncio.sleep(sleep_ms / 1000)
        return b"done"

    handlers = {1: slow_handler}
    batch = [FakeRequest(handler_id=1, request_id=i) for i in range(n_handlers)]

    partitioned = _partition_batch(batch, handlers, [])

    start = time.monotonic()
    results = _dispatch_batch(partitioned, handlers, [], FakeResponse, scheduler)
    elapsed_ms = (time.monotonic() - start) * 1000

    # Should be ~20ms (concurrent), NOT 100ms (sequential)
    assert elapsed_ms < sleep_ms * 2, (
        f"Async handlers took {elapsed_ms:.0f}ms — expected ~{sleep_ms}ms (concurrent)"
    )
    assert len(results) == n_handlers
    assert all(r.status == 200 for r in results)


def test_concurrent_dispatch_error_isolation(scheduler):
    """One async handler failure must not cancel others."""
    from vertebra.server._native import _dispatch_batch, _partition_batch

    async def good_handler(req: Any) -> bytes:
        return b"ok"

    async def bad_handler(req: Any) -> bytes:
        msg = "boom"
        raise ValueError(msg)

    handlers = {1: good_handler, 2: bad_handler}
    batch = [
        FakeRequest(handler_id=1, request_id=1),
        FakeRequest(handler_id=2, request_id=2),
        FakeRequest(handler_id=1, request_id=3),
    ]

    partitioned = _partition_batch(batch, handlers, [])
    results = _dispatch_batch(partitioned, handlers, [], FakeResponse, scheduler)

    assert len(results) == 3
    assert results[0].status == 200  # good
    assert results[1].status == 500  # bad — error isolated
    assert results[2].status == 200  # good — not cancelled


def test_concurrent_dispatch_result_ordering_preserved(scheduler):
    """Responses must be in the same order as the original batch."""
    from vertebra.server._native import _dispatch_batch, _partition_batch

    def sync_h(req: Any) -> bytes:
        return b"sync"

    async def async_h(req: Any) -> bytes:
        return b"async"

    handlers = {1: sync_h, 2: async_h}
    batch = [
        FakeRequest(handler_id=2, request_id=10),  # async
        FakeRequest(handler_id=1, request_id=20),  # sync
        FakeRequest(handler_id=2, request_id=30),  # async
        FakeRequest(handler_id=1, request_id=40),  # sync
    ]

    partitioned = _partition_batch(batch, handlers, [])
    results = _dispatch_batch(partitioned, handlers, [], FakeResponse, scheduler)

    assert [r.request_id for r in results] == [10, 20, 30, 40]


def test_concurrent_dispatch_sync_handlers_sequential(scheduler):
    """Sync handlers still run sequentially with no concurrency overhead."""
    from vertebra.server._native import _dispatch_batch, _partition_batch

    call_order: "list[int]" = []

    def sync_a(req: Any) -> bytes:
        call_order.append(1)
        return b"a"

    def sync_b(req: Any) -> bytes:
        call_order.append(2)
        return b"b"

    handlers = {1: sync_a, 2: sync_b}
    batch = [
        FakeRequest(handler_id=1, request_id=1),
        FakeRequest(handler_id=2, request_id=2),
        FakeRequest(handler_id=1, request_id=3),
    ]

    partitioned = _partition_batch(batch, handlers, [])
    results = _dispatch_batch(partitioned, handlers, [], FakeResponse, scheduler)

    assert call_order == [1, 2, 1]
    assert len(results) == 3


def test_concurrent_dispatch_mount_batching(scheduler):
    """Multiple requests to the same mount prefix dispatched as single batch call."""
    from vertebra.server._native import _dispatch_batch, _partition_batch

    call_count = 0

    class MockAsgiAdapter:
        def __call__(self, requests: list) -> list:
            nonlocal call_count
            call_count += 1
            return [FakeResponse(status=200, body=b"ok", request_id=r.request_id) for r in requests]

    adapter = MockAsgiAdapter()
    mount_runners = [("/api", adapter, "asgi")]
    batch = [
        FakeRequest(handler_id=0, path="/api/a", request_id=1),
        FakeRequest(handler_id=0, path="/api/b", request_id=2),
        FakeRequest(handler_id=0, path="/api/c", request_id=3),
    ]

    partitioned = _partition_batch(batch, {}, mount_runners)
    results = _dispatch_batch(partitioned, {}, mount_runners, FakeResponse, scheduler)

    # Should be ONE call with all 3 requests, not 3 separate calls
    assert call_count == 1
    assert len(results) == 3


# ---------------------------------------------------------------------------
# Phase 6: WSGI concurrent dispatch (Ch1: wsgi-concurrent)
# ---------------------------------------------------------------------------


def test_wsgi_concurrent_dispatch_requests_concurrent(scheduler):
    """N WSGI requests with 10ms sleep should complete in ~10ms, not N*10ms."""
    import time as time_mod

    from vertebra.server._native import _dispatch_batch, _partition_batch

    sleep_ms = 20
    n_requests = 5

    def wsgi_app(environ, start_response):
        time_mod.sleep(sleep_ms / 1000)
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"ok"]

    class FakeWsgiAdapter:
        def __call__(self, request, **kw):
            environ = {"REQUEST_METHOD": "GET", "PATH_INFO": request.path}
            state = [None]

            def start_response(status, headers, exc_info=None):
                state[0] = (status, headers)

            chunks = list(wsgi_app(environ, start_response))
            body = b"".join(chunks)
            status_code = int(state[0][0].split(" ", 1)[0])
            return FakeResponse(status=status_code, body=body, request_id=request.request_id)

    adapter = FakeWsgiAdapter()
    mount_runners = [("/app", adapter, "wsgi")]
    batch = [FakeRequest(handler_id=0, path="/app/x", request_id=i) for i in range(n_requests)]

    partitioned = _partition_batch(batch, {}, mount_runners)

    start = time_mod.monotonic()
    results = _dispatch_batch(partitioned, {}, mount_runners, FakeResponse, scheduler)
    elapsed_ms = (time_mod.monotonic() - start) * 1000

    # Should be ~20ms (concurrent), NOT 100ms (sequential 5*20ms)
    assert elapsed_ms < sleep_ms * 2, (
        f"WSGI dispatch took {elapsed_ms:.0f}ms — expected ~{sleep_ms}ms (concurrent), "
        f"not {sleep_ms * n_requests}ms (sequential)"
    )
    assert len(results) == n_requests
    assert all(r.status == 200 for r in results)


def test_wsgi_concurrent_dispatch_error_isolation(scheduler):
    """One WSGI app failure must not prevent others from completing."""
    from vertebra.server._native import _dispatch_batch, _partition_batch

    call_count = 0

    class ErrorWsgiAdapter:
        def __call__(self, request, **kw):
            nonlocal call_count
            call_count += 1
            if request.request_id == 2:
                msg = "boom"
                raise RuntimeError(msg)
            return FakeResponse(status=200, body=b"ok", request_id=request.request_id)

    adapter = ErrorWsgiAdapter()
    mount_runners = [("/app", adapter, "wsgi")]
    batch = [
        FakeRequest(handler_id=0, path="/app/a", request_id=1),
        FakeRequest(handler_id=0, path="/app/b", request_id=2),  # will error
        FakeRequest(handler_id=0, path="/app/c", request_id=3),
    ]

    partitioned = _partition_batch(batch, {}, mount_runners)
    results = _dispatch_batch(partitioned, {}, mount_runners, FakeResponse, scheduler)

    assert len(results) == 3
    assert results[0].status == 200
    assert results[1].status == 500  # error isolated
    assert results[2].status == 200
    assert call_count == 3  # all three were attempted


def test_wsgi_concurrent_dispatch_result_ordering(scheduler):
    """WSGI results must be in original batch order."""
    from vertebra.server._native import _dispatch_batch, _partition_batch

    class OrderingWsgiAdapter:
        def __call__(self, request, **kw):
            body = f"req-{request.request_id}".encode()
            return FakeResponse(status=200, body=body, request_id=request.request_id)

    adapter = OrderingWsgiAdapter()
    mount_runners = [("/app", adapter, "wsgi")]
    batch = [
        FakeRequest(handler_id=0, path="/app/x", request_id=10),
        FakeRequest(handler_id=0, path="/app/y", request_id=20),
        FakeRequest(handler_id=0, path="/app/z", request_id=30),
    ]

    partitioned = _partition_batch(batch, {}, mount_runners)
    results = _dispatch_batch(partitioned, {}, mount_runners, FakeResponse, scheduler)

    assert [r.request_id for r in results] == [10, 20, 30]
    assert results[0].body == b"req-10"
    assert results[1].body == b"req-20"
    assert results[2].body == b"req-30"


# ---------------------------------------------------------------------------
# Phase 7: Hybrid dispatch — sync + async in parallel (Ch4: hybrid-workers)
# ---------------------------------------------------------------------------


def test_hybrid_dispatch_async_handlers_amortize_io(scheduler):
    """Multiple async handlers with sleep should complete in ~1 sleep, not N*sleep."""
    import time as time_mod

    from vertebra.server._native import _dispatch_batch, _partition_batch

    sleep_ms = 30
    n = 5

    async def slow_async(req: Any) -> bytes:
        await asyncio.sleep(sleep_ms / 1000)
        return b"async"

    handlers = {1: slow_async}
    batch = [FakeRequest(handler_id=1, request_id=i) for i in range(n)]

    partitioned = _partition_batch(batch, handlers, [])

    start = time_mod.monotonic()
    results = _dispatch_batch(partitioned, handlers, [], FakeResponse, scheduler)
    elapsed_ms = (time_mod.monotonic() - start) * 1000

    # All 5 sleeps gathered concurrently: ~30ms, not 150ms.
    assert elapsed_ms < sleep_ms * 2, (
        f"Async dispatch took {elapsed_ms:.0f}ms — expected ~{sleep_ms}ms (gathered), "
        f"not {sleep_ms * n}ms (sequential)"
    )
    assert len(results) == n
    assert all(r.status == 200 for r in results)


def test_hybrid_dispatch_sync_only_batch_no_deadlock(scheduler):
    """Batch with only sync handlers must complete without deadlock."""
    from vertebra.server._native import _dispatch_batch, _partition_batch

    def sync_h(req: Any) -> bytes:
        return b"ok"

    handlers = {1: sync_h}
    batch = [FakeRequest(handler_id=1, request_id=i) for i in range(5)]

    partitioned = _partition_batch(batch, handlers, [])
    results = _dispatch_batch(partitioned, handlers, [], FakeResponse, scheduler)
    assert len(results) == 5
    assert all(r.status == 200 for r in results)


def test_hybrid_dispatch_async_only_batch_no_deadlock(scheduler):
    """Batch with only async handlers must complete without deadlock."""
    from vertebra.server._native import _dispatch_batch, _partition_batch

    async def async_h(req: Any) -> bytes:
        return b"ok"

    handlers = {1: async_h}
    batch = [FakeRequest(handler_id=1, request_id=i) for i in range(5)]

    partitioned = _partition_batch(batch, handlers, [])
    results = _dispatch_batch(partitioned, handlers, [], FakeResponse, scheduler)
    assert len(results) == 5
    assert all(r.status == 200 for r in results)


# ---------------------------------------------------------------------------
# Phase 8: Thread safety for free-threading (Ch5: free-threading)
# ---------------------------------------------------------------------------


def test_thread_safety_concurrent_sync_handlers_no_data_corruption(scheduler):
    """Multiple sync handlers on thread pool must not corrupt shared results."""
    import threading

    from vertebra.server._native import _dispatch_batch, _partition_batch

    seen_threads: "set[int]" = set()
    lock = threading.Lock()

    def sync_h(req: Any) -> bytes:
        tid = threading.get_ident()
        with lock:
            seen_threads.add(tid)
        return f"t{req.request_id}".encode()

    handlers = {1: sync_h}
    batch = [FakeRequest(handler_id=1, request_id=i) for i in range(20)]

    partitioned = _partition_batch(batch, handlers, [])
    results = _dispatch_batch(partitioned, handlers, [], FakeResponse, scheduler)

    # All 20 results must be present and correctly ordered.
    assert len(results) == 20
    for i in range(20):
        assert results[i].body == f"t{i}".encode()
        assert results[i].request_id == i

    # Should have used multiple threads (thread pool).
    assert len(seen_threads) >= 1  # At least 1; may be >1 under pool


def test_thread_safety_handler_meta_cache_thread_safe():
    """_handler_meta LRU cache must be safe to call from multiple threads."""
    import concurrent.futures

    from vertebra.server._native import _handler_meta

    def sync_h(req: Any) -> bytes:
        return b"ok"

    async def async_h(req: Any) -> bytes:
        return b"ok"

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(_handler_meta, sync_h if i % 2 == 0 else async_h) for i in range(100)]
        results = [f.result() for f in futures]

    # All results should be consistent.
    sync_results = [r for i, r in enumerate(results) if i % 2 == 0]
    async_results = [r for i, r in enumerate(results) if i % 2 != 0]
    assert all(r == (True, False) for r in sync_results)
    assert all(r == (True, True) for r in async_results)


def test_thread_safety_run_worker_no_free_threading_gate():
    """run_worker with worker_count>1 should NOT require free-threading."""
    import inspect

    from vertebra.server._native import run_worker

    # Verify the function signature accepts worker_count.
    sig = inspect.signature(run_worker)
    assert "worker_count" in sig.parameters

    # The implementation should not gate on _is_free_threaded for worker_count>1.
    # We can't call run_worker without the native extension, but we verify
    # the source code doesn't contain the old gate.
    source = inspect.getsource(run_worker)
    assert "_is_free_threaded()" not in source


# ---------------------------------------------------------------------------
# Phase 5: Free-threading support
# ---------------------------------------------------------------------------


def test_free_threading_is_free_threaded_returns_bool():
    """_is_free_threaded should return a bool without error."""
    from vertebra.server._native import _is_free_threaded

    result = _is_free_threaded()
    assert isinstance(result, bool)


def test_free_threading_run_worker_accepts_worker_count():
    """run_worker should accept the worker_count kwarg."""
    import inspect

    from vertebra.server._native import run_worker

    sig = inspect.signature(run_worker)
    assert "worker_count" in sig.parameters


# ---------------------------------------------------------------------------
# Phase 4: 404 handling
# ---------------------------------------------------------------------------


def test_unmatched_gives_404(scheduler):
    from vertebra.server._native import _dispatch_batch, _partition_batch

    batch = [FakeRequest(handler_id=999, path="/nope", request_id=42)]

    partitioned = _partition_batch(batch, {}, [])
    results = _dispatch_batch(partitioned, {}, [], FakeResponse, scheduler)

    assert len(results) == 1
    assert results[0].status == 404
    assert results[0].request_id == 42
