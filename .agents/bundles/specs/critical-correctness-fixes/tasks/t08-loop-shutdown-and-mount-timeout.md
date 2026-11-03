---
type: Task
id: critical-correctness-fixes:t08
title: EventLoopPool.stop drain and configurable ASGI mount timeout
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on: []
files:
  - src/py/vertebra/server/_loops.py
  - src/py/vertebra/server/worker.py
  - docs/usage/server-and-workers.md
  - .agents/bundles/knowledge/architecture/runtime/platform-and-lifecycle.md
  - .agents/bundles/knowledge/domains/server/worker-and-protocols.md
  - docs/changelog/index.md
tests:
  - src/py/tests/unit/server/test_runner.py
plan_revision: 2
plan_commit: 24e2931
state_revision: 2
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: 2026-11-03T17:09:22Z
last_verified_commit: 21ff97a
verification_evidence: "RED: 11 failed, 1 passed in 1.21s; GREEN: 254 passed in 2.33s; ruff check/format and mypy passed on 2 source files"
created_at: 2026-11-03T15:30:00Z
updated_at: 2026-11-03T17:12:00Z
commit: 5edc876
tags: [bugfix, server, event-loop, asgi]
---

# t08 — `EventLoopPool.stop` drain and configurable ASGI mount timeout

## Objective

`EventLoopPool` shuts down cleanly (cancellations delivered, async generators finalized, loops closed only after their threads exit) and fails fast when a shard never starts; the buffered sharded ASGI mount path takes its timeout from configuration (default 30 s, disable with `0`/`none`) and cancels the asyncio task when the timeout expires.

## Context

- **Strategy**: `regression_tdd`. All defects reproduce at `142cc49`.
- **Defects confirmed at HEAD**:
  - `src/py/vertebra/server/_loops.py::EventLoopPool.stop` schedules one `_shutdown` callback per shard that calls `t.cancel()` on tracked tasks and then `target_loop.stop()`. The cancellation wakeups land in the next loop iteration, which never runs, so `except CancelledError` / `finally` blocks do not execute. `shutdown_asyncgens` is never called. After `thread.join(timeout=2.0)` every loop is closed even if its thread is still alive. Only tasks in `_task_sets` are cancelled; tasks created by `schedule` / `schedule_batch` futures are not tracked there.
  - `EventLoopPool.__init__` calls `ready.wait(timeout=5.0)` and ignores the result.
  - `src/py/vertebra/server/worker.py::_AsgiMountRunner.__call__` waits `concurrent.futures.wait(futures, timeout=10.0)` with a hardcoded value. `fut.cancel()` cancels only the bridging `concurrent.futures.Future`; `EventLoopPool.schedule_batch` never propagates that cancellation to the asyncio task, which keeps running after the 500 response is returned.
- **Who calls `_AsgiMountRunner.__call__`** (grep of `adapter(requests_list)` / mount dispatch in `src/py/vertebra`): the only caller is `src/py/vertebra/server/_dispatch.py::dispatch_batch` in its buffered branch (`async_mode=False`, `if protocol == "asgi": responses = adapter(requests_list)`). The production worker calls `dispatch_batch(..., async_mode=True)` (`src/py/vertebra/server/worker.py::Worker._process_batch`), which sends ASGI mounts through `_AsgiMountRunner.spawn_batch` and never through `__call__`. `__call__` is therefore a buffered-only path (it returns complete `AsgiResponse` objects), used by `dispatch_batch(async_mode=False)` callers such as the unit tests in `test_runner.py`; it cannot carry SSE or streamed bodies, so its timeout never applied to streaming responses. The path is live API, not dead code, so it keeps a finite default.
- **Decisions** (spec §2.2):
  - Module constants in `_loops.py`: `_SHARD_START_TIMEOUT_SECONDS: float = 5.0` and `_STOP_TIMEOUT_SECONDS: float = 5.0` (read at call time so tests can lower them).
  - Shutdown drains every task on the shard (`asyncio.all_tasks()` minus the draining task), awaits them with `gather(return_exceptions=True)`, then `await loop.shutdown_asyncgens()`. A drain that exceeds the timeout is logged and the loop is still stopped. A loop whose thread is alive after the join timeout is logged and left open.
  - Shard start failure raises `RuntimeError(f"Event loop shard {idx} did not start within {timeout}s")` after calling `self.stop()`.
  - `_AsgiMountRunner(..., batch_timeout: float | None = 30.0)`; `None` waits without a timeout. Timed-out requests return `AsgiResponse(status=504, headers=[("content-type", "text/plain")], body=b"Gateway Timeout", request_id=<id>)` and their tasks are cancelled.
  - `_instantiate_mount_runners` resolves the timeout from `VERTEBRA_ASGI_BATCH_TIMEOUT` (seconds) through a new module function `_resolve_asgi_batch_timeout`, next to the existing `_resolve_configured_interface`. Unset or empty → `30.0`; `0` or `none` (any case) → `None`; a positive number → that value; negative or non-numeric → `ImproperConfigurationError` (from `vertebra.typing`).
- **Related, out of scope**: `dispatch_batch`'s buffered `async_native` branch has the same hardcoded `concurrent.futures.wait(futures, timeout=10.0)`; `runtime-structure-and-throughput` owns the dispatch restructure (recorded in spec Non-Goals).
- **Fault injection note**: `test_event_loop_pool_raises_when_shard_does_not_start` replaces the shard thread target `EventLoopPool._run_loop_shard` with a function that returns without signaling readiness. This simulates a shard thread that dies during startup; it does not replace the behavior under test (`__init__`'s readiness check).
- **Request carrier**: the mount runner test uses the module's existing `MockRequest` (method, path, query, headers, body, request_id). `AsgiRunner._build_scope` accepts duck-typed request objects through `_build_scope_from_stub`, so no private request class is imported.
- **Canonical test module**: `src/py/tests/unit/server/test_runner.py` (imports `EventLoopPool`, `asyncio`, `threading`, `AsyncIterator`, `Any`, `pytest`, `MockRequest`, and `vertebra.server.worker`).
- **Code rules**: `msgspec.Struct(frozen=True, gc=False)` instead of dataclasses; PEP 585/604/695 types; Google-style docstrings; zero inline `#` comments; top-level imports only (except `TYPE_CHECKING` and optional-dependency helpers); class layout attributes → `__init__` → properties → public methods → private methods; JSON through `vertebra.utils.serializers`; `@pytest.mark.parametrize` instead of loops over cases; no compatibility aliases or `try/except` fallbacks for test seeds; no finding IDs in code, tests, or commits.

## Steps

### 1. Write failing tests (RED)

In `src/py/tests/unit/server/test_runner.py`:

1. Imports: change `from vertebra.server.worker import Worker, run_worker` to `from vertebra.server.worker import Worker, _AsgiMountRunner, _instantiate_mount_runners, run_worker`; add `from vertebra.typing import ImproperConfigurationError` to the first-party imports.
2. Append:

   ```python
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
               await asyncio.sleep(3600)
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
               await asyncio.sleep(3600)
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
   ```

3. Run RED:

   ```bash
   uv run pytest src/py/tests/unit/server/test_runner.py -q -k "event_loop_pool_stop or shard_does_not_start or asgi_mount"
   ```

   Expected: the drain test fails on `assert cancelled.is_set()`; the shard-start test fails at `monkeypatch.setattr` with `AttributeError: <module 'vertebra.server._loops' ...> has no attribute '_SHARD_START_TIMEOUT_SECONDS'`; the timeout test fails with `TypeError: ... unexpected keyword argument 'batch_timeout'`; the environment tests fail with `AttributeError: '_AsgiMountRunner' object has no attribute 'batch_timeout'` or `DID NOT RAISE`.

### 2. Implement (GREEN)

1. `src/py/vertebra/server/_loops.py`:
   - Add `import logging` to the imports and, after the imports, `logger = logging.getLogger(__name__)`, `_SHARD_START_TIMEOUT_SECONDS: float = 5.0`, `_STOP_TIMEOUT_SECONDS: float = 5.0`.
   - `EventLoopPool.__init__`: replace `ready.wait(timeout=5.0)` with

   ```python
   if not ready.wait(timeout=_SHARD_START_TIMEOUT_SECONDS):
       self.stop()
       msg = f"Event loop shard {shard_idx} did not start within {_SHARD_START_TIMEOUT_SECONDS}s"
       raise RuntimeError(msg)
   ```

   - `schedule_batch._schedule_shard`: at the top of the `for c, f in shard_entries:` body add

   ```python
   if f.cancelled():
       c.close()
       continue
   ```

   and after `task.add_done_callback(_on_done)` add

   ```python
   def _cancel_task(
       done: concurrent.futures.Future[T],
       target_task: asyncio.Task[T] = task,
       owner_loop: asyncio.AbstractEventLoop = target_loop,
   ) -> None:
       if done.cancelled() and not owner_loop.is_closed():
           owner_loop.call_soon_threadsafe(target_task.cancel)

   f.add_done_callback(_cancel_task)
   ```

   - Replace `stop` with:

   ```python
   def stop(self) -> None:
       """Drain and stop every loop shard, closing each loop only after its thread exits.

       Each running shard cancels its pending tasks, waits for the cancellations, and runs
       ``shutdown_asyncgens`` before the loop stops. A loop whose thread is still alive after the
       join timeout is logged and left open rather than closed under a running thread.
       """
       if self._stopped:
           return
       self._stopped = True

       if self._inline_single:
           loop = self._loops[0]
           if loop.is_running():
               loop.call_soon_threadsafe(loop.stop)
           elif not loop.is_closed():
               try:
                   loop.run_until_complete(asyncio.wait_for(self._drain_loop(), _STOP_TIMEOUT_SECONDS))
               except TimeoutError:
                   logger.warning("Inline event loop did not drain within %.1fs", _STOP_TIMEOUT_SECONDS)
               loop.close()
           return

       for idx, loop in enumerate(self._loops):
           if not loop.is_running():
               continue
           drain = asyncio.run_coroutine_threadsafe(self._drain_loop(), loop)
           try:
               drain.result(timeout=_STOP_TIMEOUT_SECONDS)
           except TimeoutError:
               logger.warning("Event loop shard %d did not drain within %.1fs", idx, _STOP_TIMEOUT_SECONDS)
           loop.call_soon_threadsafe(loop.stop)

       for thread in self._threads:
           thread.join(timeout=_STOP_TIMEOUT_SECONDS)

       for loop, thread in zip(self._loops, self._threads, strict=True):
           if thread.is_alive():
               logger.warning("Event loop thread %s is still running; leaving its loop open", thread.name)
               continue
           if not loop.is_closed():
               loop.close()
   ```

   - Add a private static method after `stop` (before `_run_loop_shard`):

   ```python
   @staticmethod
   async def _drain_loop() -> None:
       """Cancel every other task on the running loop, await the cancellations, and finalize async generators."""
       current = asyncio.current_task()
       pending = [task for task in asyncio.all_tasks() if task is not current]
       for task in pending:
           task.cancel()
       if pending:
           await asyncio.gather(*pending, return_exceptions=True)
       await asyncio.get_running_loop().shutdown_asyncgens()
   ```

2. `src/py/vertebra/server/worker.py`:
   - Add `from vertebra.typing import ImproperConfigurationError` to the first-party imports.
   - Add module constant after `_INLINE_DRAIN_BUDGET = 16`: `_DEFAULT_ASGI_BATCH_TIMEOUT_SECONDS: float = 30.0`.
   - `_AsgiMountRunner`: `__slots__ = ("_batch_timeout", "_pool", "_runner")`; `__init__` gains keyword `batch_timeout: float | None = _DEFAULT_ASGI_BATCH_TIMEOUT_SECONDS` stored as `self._batch_timeout`; document it in the docstring ("Seconds to wait for a buffered batch before answering 504 and cancelling the request tasks. `None` waits without a timeout."). Add property after `runner`:

   ```python
   @property
   def batch_timeout(self) -> float | None:
       """Return the batch timeout in seconds, or None when batches wait without a timeout."""
       return self._batch_timeout
   ```

   - Replace `__call__` with:

   ```python
   def __call__(self, requests: list[Any]) -> list[AsgiResponse]:
       """Run a batch across the sharded loop pool, answering 504 for requests past ``batch_timeout``."""
       if not requests:
           return []
       coros = [self._runner.run_single(req) for req in requests]
       futures = self._pool.schedule_batch(coros)
       _done, not_done = concurrent.futures.wait(futures, timeout=self._batch_timeout)
       for fut in not_done:
           fut.cancel()
       results: list[AsgiResponse] = []
       for req, fut in zip(requests, futures, strict=True):
           req_id = getattr(req, "request_id", 0)
           if fut in not_done:
               results.append(
                   AsgiResponse(
                       status=504,
                       headers=[("content-type", "text/plain")],
                       body=b"Gateway Timeout",
                       request_id=req_id,
                   )
               )
               continue
           try:
               results.append(fut.result())
           except Exception:
               logger.exception("Sharded ASGI mount request failed (request_id=%d)", req_id)
               results.append(
                   AsgiResponse(
                       status=500,
                       headers=[("content-type", "text/plain")],
                       body=b"Internal Server Error",
                       request_id=req_id,
                   )
               )
       return results
   ```

   - `_instantiate_mount_runners`: after `configured_iface = _resolve_configured_interface()` add `asgi_batch_timeout = _resolve_asgi_batch_timeout()`, and pass `batch_timeout=asgi_batch_timeout` to `_AsgiMountRunner(...)`.
   - Add after `_resolve_configured_interface`:

   ```python
   def _resolve_asgi_batch_timeout() -> float | None:
       """Resolve ``VERTEBRA_ASGI_BATCH_TIMEOUT`` (seconds) for buffered sharded ASGI mount batches.

       Unset or empty means the 30 second default. ``0`` or ``none`` (any case) disables the
       timeout. Any other value must be a positive number of seconds.

       Raises:
           ImproperConfigurationError: When the value is negative or not a number.
       """
       raw = os.environ.get("VERTEBRA_ASGI_BATCH_TIMEOUT", "").strip()
       if not raw:
           return _DEFAULT_ASGI_BATCH_TIMEOUT_SECONDS
       if raw.lower() == "none":
           return None
       msg = f"VERTEBRA_ASGI_BATCH_TIMEOUT must be a non-negative number of seconds or 'none', got {raw!r}"
       try:
           timeout = float(raw)
       except ValueError as exc:
           raise ImproperConfigurationError(msg) from exc
       if timeout < 0:
           raise ImproperConfigurationError(msg)
       return None if timeout == 0 else timeout
   ```

3. Docs `docs/usage/server-and-workers.md`: add a section before the free-threading section:

   ```markdown
   ## ASGI Mount Timeouts

   When an ASGI mount is served through the buffered batch path, each request waits up to 30 seconds by default. Set `VERTEBRA_ASGI_BATCH_TIMEOUT` to another number of seconds, or to `0` or `none` to disable the limit. A request that exceeds it receives `504 Gateway Timeout` and its task is cancelled. Streaming responses use a separate path and are not affected.
   ```

4. Knowledge:
   - `.agents/bundles/knowledge/architecture/runtime/platform-and-lifecycle.md`, `EventLoopPool` bullet: append "`stop()` cancels and awaits every pending task, runs `shutdown_asyncgens`, stops each loop, and closes a loop only after its thread exits; `__init__` raises `RuntimeError` when a shard does not signal readiness within 5 s."
   - Same file, "Per-shard ASGI mounts" bullet: append "The worker dispatches mounts through `spawn_batch`; `_AsgiMountRunner.__call__` is the buffered path used by `dispatch_batch(async_mode=False)`. It waits up to `VERTEBRA_ASGI_BATCH_TIMEOUT` (default 30 s, `0`/`none` disables); timed-out requests get `504` and their tasks are cancelled through `schedule_batch` future cancellation."
   - `.agents/bundles/knowledge/domains/server/worker-and-protocols.md`, `EventLoopPool` table row: append "Shutdown drains tasks and async generators before closing loops."
5. Changelog `docs/changelog/index.md`, `## 0.3.0` → `### Fixed`, append: `` - `EventLoopPool.stop` delivers task cancellation, runs `shutdown_asyncgens`, and closes loops only after their threads exit; `EventLoopPool` raises when a shard fails to start; the buffered sharded ASGI mount path reads its timeout from `VERTEBRA_ASGI_BATCH_TIMEOUT` (default 30 s, `0`/`none` disables) instead of a fixed 10 s and cancels timed-out request tasks. ``

### 3. Refactor & verify

1. Run the GREEN commands; run `make lint`.
2. Commit:

   ```bash
   git add src/py/vertebra/server/_loops.py src/py/vertebra/server/worker.py src/py/tests/unit/server/test_runner.py docs/usage/server-and-workers.md .agents/bundles/knowledge/architecture/runtime/platform-and-lifecycle.md .agents/bundles/knowledge/domains/server/worker-and-protocols.md docs/changelog/index.md
   git commit -S -m "fix(server): drain event loops on stop and make the ASGI mount timeout configurable"
   ```

## Verification

- **RED**: the Step 1.3 command exits `1` with the failures listed.
- **GREEN**:

  ```bash
  uv run pytest src/py/tests/unit/server -q
  uv run ruff check src/py/vertebra/server/_loops.py src/py/vertebra/server/worker.py src/py/tests/unit/server/test_runner.py
  uv run ruff format --check src/py/vertebra/server/_loops.py src/py/vertebra/server/worker.py src/py/tests/unit/server/test_runner.py
  uv run mypy src/py/vertebra/server/_loops.py src/py/vertebra/server/worker.py
  ```

  All exit `0`.
- **Phase gate** (Phase 2 checkpoint, after this task):

  ```bash
  make lint-all
  make py-test
  ```

  Both exit `0`.

## Acceptance Criteria

- [x] R12: `stop()` runs task cancellation handlers and async-generator finalizers on every shard and closes loops only after their threads exit.
- [x] R12: `EventLoopPool(size>1)` raises `RuntimeError` when a shard does not signal readiness.
- [x] R12: Cancelling a `schedule_batch` future cancels its asyncio task.
- [x] R13: `_AsgiMountRunner` defaults to a 30 s timeout, reads `VERTEBRA_ASGI_BATCH_TIMEOUT` (`0`/`none` disables), rejects negative and non-numeric values, returns 504 on timeout, and cancels the task.
- [x] Docs, knowledge, and changelog updated; targeted checks and the phase gate pass; one signed commit.

## Notes & Discoveries

- [2026-11-03 15:30] Planning: `concurrent.futures.Future.cancel()` on the futures returned by `schedule_batch` succeeds (they stay `PENDING`) but never reached the asyncio task before this change.
- [2026-11-03 15:50] Caller grep: `_AsgiMountRunner.__call__` is reached only from `server/_dispatch.py::dispatch_batch` with `async_mode=False`; `Worker._process_batch` uses `async_mode=True`, which routes ASGI mounts to `spawn_batch`. The buffered path never served streaming responses.
- [2026-11-03 17:09] Verified RED (`11 failed, 1 passed in 1.21s`) and GREEN (`254 passed in 2.33s`, `ruff check`, `ruff format --check`, and `mypy` passed on 2 source files).
