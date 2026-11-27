"""Concurrency, inline fastpath, error isolation, and shutdown tests for the adaptive WSGI pool."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Generator, Iterable
from typing import Any

import pytest

from vertebra._vertebra import (
    FREE_THREADED,
    Request,
    drive_wsgi_batch,
    init_transport,
    shutdown_wsgi_executor,
)
from vertebra.server._wsgi._adapter import WsgiAdapter
from vertebra.server.worker import Worker


@pytest.fixture(autouse=True)
def _reset_wsgi_executor() -> Generator[None, None, None]:
    """Ensure every test starts and ends with a fresh native WSGI executor."""
    init_transport()
    shutdown_wsgi_executor()
    yield
    shutdown_wsgi_executor()


def _make_requests(count: int, *, prefix: str = "/work") -> list[Request]:
    """Create a batch of distinct native Request instances."""
    return [Request.from_parts("GET", f"{prefix}/{idx}", request_id=idx + 1) for idx in range(count)]


def test_auto_mode_blocking_io_runs_concurrently_under_wall_clock_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify 16 x 30 ms blocking WSGI requests complete in parallel under 150 ms."""
    monkeypatch.setenv("VERTEBRA_WSGI_MODE", "auto")
    monkeypatch.setenv("VERTEBRA_WSGI_THREADS", "16")
    monkeypatch.setenv("VERTEBRA_WSGI_STALL_US", "250")

    completed_paths: list[str] = []
    lock = threading.Lock()
    all_done = threading.Event()
    total = 16

    def blocking_app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        path_info = str(environ["PATH_INFO"])
        time.sleep(0.03)
        start_response("200 OK", [("Content-Type", "text/plain")])
        with lock:
            completed_paths.append(path_info)
            if len(completed_paths) == total:
                all_done.set()
        return [b"ok"]

    adapter = WsgiAdapter(blocking_app)
    requests = _make_requests(total)

    started = time.perf_counter()
    dispatched = adapter.spawn_batch(requests)
    assert all_done.wait(timeout=2.0), "timed out waiting for 16 concurrent WSGI responses"
    elapsed_ms = (time.perf_counter() - started) * 1000.0

    assert dispatched == total
    assert len(completed_paths) == total
    assert len(set(completed_paths)) == total
    assert elapsed_ms < 150.0, f"expected parallel completion < 150 ms, got {elapsed_ms:.1f} ms"


@pytest.mark.parametrize("mode", ["auto", "concurrent"])
def test_wsgi_pool_barrier_proves_simultaneous_execution(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    """Verify at least 4 WSGI requests execute simultaneously via a threading.Barrier."""
    monkeypatch.setenv("VERTEBRA_WSGI_MODE", mode)
    monkeypatch.setenv("VERTEBRA_WSGI_THREADS", "8")
    monkeypatch.setenv("VERTEBRA_WSGI_STALL_US", "250")

    barrier = threading.Barrier(4, timeout=1.5)
    entered: list[int] = []
    lock = threading.Lock()
    done = threading.Event()

    def barrier_app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        del environ
        barrier.wait()
        start_response("200 OK", [("Content-Type", "text/plain")])
        with lock:
            entered.append(threading.get_ident())
            if len(entered) == 4:
                done.set()
        return [b"released"]

    dispatched = drive_wsgi_batch(barrier_app, _make_requests(4))
    assert done.wait(timeout=2.0), f"barrier did not release in mode={mode!r}"
    assert dispatched == 4
    assert len(entered) == 4


def test_auto_mode_zero_io_fastpath_and_shared_server_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify default auto mode runs fast zero-I/O batches inline on GIL builds and preserves WsgiServerConfig."""
    monkeypatch.delenv("VERTEBRA_WSGI_MODE", raising=False)
    monkeypatch.delenv("VERTEBRA_WSGI_STALL_US", raising=False)

    caller_tid = threading.get_ident()
    seen_tids: list[int] = []
    seen_meta: list[tuple[str, str, str, str, str]] = []
    lock = threading.Lock()
    done = threading.Event()
    total = 8

    def fast_app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        meta = (
            str(environ["SCRIPT_NAME"]),
            str(environ["PATH_INFO"]),
            str(environ["SERVER_NAME"]),
            str(environ["SERVER_PORT"]),
            str(environ["wsgi.url_scheme"]),
        )
        start_response("200 OK", [("Content-Type", "text/plain")])
        with lock:
            seen_tids.append(threading.get_ident())
            seen_meta.append(meta)
            if len(seen_tids) == total:
                done.set()
        return [b"ok"]

    requests = [Request.from_parts("GET", f"/api/item/{idx}", request_id=idx + 1) for idx in range(total)]
    dispatched = drive_wsgi_batch(
        fast_app,
        requests,
        root_path="/api/",
        server_name="edge.local",
        server_port=8443,
        url_scheme="https",
    )
    assert done.wait(timeout=2.0)
    assert dispatched == total
    assert len(seen_meta) == total
    for idx in range(total):
        assert ("/api", f"/item/{idx}", "edge.local", "8443", "https") in seen_meta
    if not FREE_THREADED:
        assert all(tid == caller_tid for tid in seen_tids)
    else:
        assert any(tid != caller_tid for tid in seen_tids)


def test_inline_mode_override_runs_synchronously_on_caller_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify VERTEBRA_WSGI_MODE=inline forces synchronous caller-thread execution."""
    monkeypatch.setenv("VERTEBRA_WSGI_MODE", "inline")
    caller_tid = threading.get_ident()
    observed_tids: list[int] = []

    def slow_inline_app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        del environ
        time.sleep(0.002)
        observed_tids.append(threading.get_ident())
        start_response("200 OK", [])
        return [b"inline"]

    dispatched = drive_wsgi_batch(slow_inline_app, _make_requests(4))
    assert dispatched == 4
    assert observed_tids == [caller_tid] * 4


def test_auto_mode_hysteresis_routes_followup_batch_to_pool_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a stalled batch activates the 2 s concurrent hysteresis window for subsequent batches."""
    monkeypatch.setenv("VERTEBRA_WSGI_MODE", "auto")
    monkeypatch.setenv("VERTEBRA_WSGI_THREADS", "8")
    monkeypatch.setenv("VERTEBRA_WSGI_STALL_US", "250")

    caller_tid = threading.get_ident()

    def trigger_stall(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        del environ
        time.sleep(0.005)
        start_response("200 OK", [])
        return [b"stalled"]

    drive_wsgi_batch(trigger_stall, _make_requests(1, prefix="/stall"))

    followup_tids: list[int] = []
    lock = threading.Lock()
    done = threading.Event()
    total = 4

    def fast_followup(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        del environ
        start_response("200 OK", [])
        with lock:
            followup_tids.append(threading.get_ident())
            if len(followup_tids) == total:
                done.set()
        return [b"fast"]

    drive_wsgi_batch(fast_followup, _make_requests(total, prefix="/after"))
    assert done.wait(timeout=2.0)
    assert len(followup_tids) == total
    assert all(tid != caller_tid for tid in followup_tids)


def test_concurrent_pool_isolates_exceptions_and_pep3333_violations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify raising handlers and missing start_response do not poison pool threads or drop healthy items."""
    monkeypatch.setenv("VERTEBRA_WSGI_MODE", "concurrent")
    monkeypatch.setenv("VERTEBRA_WSGI_THREADS", "4")

    healthy_seen: list[str] = []
    lock = threading.Lock()
    done = threading.Event()

    def mixed_app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        path_info = str(environ["PATH_INFO"])
        if path_info == "/boom":
            msg = "intentional failure"
            raise RuntimeError(msg)
        if path_info == "/no-start":
            return [b"missing start_response"]
        start_response("200 OK", [("Content-Type", "text/plain")])
        with lock:
            healthy_seen.append(path_info)
            if len(healthy_seen) == 2:
                done.set()
        return [b"healthy"]

    batch = [
        Request.from_parts("GET", "/boom", request_id=1),
        Request.from_parts("GET", "/ok-1", request_id=2),
        Request.from_parts("GET", "/no-start", request_id=3),
        Request.from_parts("GET", "/ok-2", request_id=4),
    ]
    assert drive_wsgi_batch(mixed_app, batch) == 4
    assert done.wait(timeout=2.0)
    assert sorted(healthy_seen) == ["/ok-1", "/ok-2"]

    followup_done = threading.Event()

    def healthy_followup(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        del environ
        start_response("200 OK", [])
        followup_done.set()
        return [b"still-alive"]

    assert drive_wsgi_batch(healthy_followup, _make_requests(1, prefix="/followup")) == 1
    assert followup_done.wait(timeout=2.0)


def test_shutdown_wsgi_executor_drains_inflight_without_gil_deadlock_and_reinitializes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify shutdown_wsgi_executor drains in-flight blocking tasks cleanly and allows lazy re-initialization."""
    monkeypatch.setenv("VERTEBRA_WSGI_MODE", "concurrent")
    monkeypatch.setenv("VERTEBRA_WSGI_THREADS", "8")

    completed = 0
    lock = threading.Lock()
    total = 8

    def blocking_app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        nonlocal completed
        del environ
        time.sleep(0.025)
        start_response("200 OK", [])
        with lock:
            completed += 1
        return [b"drained"]

    assert drive_wsgi_batch(blocking_app, _make_requests(total)) == total
    shutdown_wsgi_executor()
    assert completed == total

    reinit_done = threading.Event()

    def after_reinit(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        del environ
        start_response("200 OK", [])
        reinit_done.set()
        return [b"reinitialized"]

    assert drive_wsgi_batch(after_reinit, _make_requests(1, prefix="/reinit")) == 1
    assert reinit_done.wait(timeout=2.0)


def test_worker_run_calls_shutdown_wsgi_executor_in_finally(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Worker.run invokes shutdown_wsgi_executor on both clean stop and startup error."""
    import vertebra.server.worker as worker_mod

    calls: list[str] = []

    def record_shutdown() -> None:
        calls.append("shutdown")
        shutdown_wsgi_executor()

    monkeypatch.setattr(worker_mod, "shutdown_wsgi_executor", record_shutdown)
    monkeypatch.setattr(worker_mod, "init_transport", lambda *_a, **_kw: None)
    monkeypatch.setattr(worker_mod, "get_request_notify_fd", lambda: -1)

    worker = Worker({}, max_batches=0)
    worker.run()
    assert calls == ["shutdown"]

    def fail_lifespan(*_args: Any, **_kwargs: Any) -> Any:
        msg = "simulated startup failure"
        raise RuntimeError(msg)

    monkeypatch.setattr(worker_mod, "enter_litestar_server_lifespan", fail_lifespan)
    failing_worker = Worker({}, mounts=[("/bad", object(), "asgi")], max_batches=0)
    with pytest.raises(RuntimeError):
        failing_worker.run()
    assert calls == ["shutdown", "shutdown"]
