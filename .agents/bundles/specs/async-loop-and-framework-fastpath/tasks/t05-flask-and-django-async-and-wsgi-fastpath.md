---
type: Task
id: async-loop-and-framework-fastpath:t05
title: "Add async def view execution, cached MsgspecView, wrap_asgi, and _WsgiStartResponse.send_raw_response to Flask and Django FastPath"
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - async-loop-and-framework-fastpath:t04
files:
  - src/py/vertebra/extensions/flask/fastpath.py
  - src/py/vertebra/extensions/django/fastpath.py
tests:
  - src/py/tests/unit/extensions/test_flask.py
  - src/py/tests/unit/extensions/test_django.py
plan_revision: 2
plan_commit: 34429ff8c50d4622d0cacbd1288525f8a1211dc8
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: 2026-11-01T23:48:00Z
updated_at: 2026-11-02T00:03:00Z
commit: null
tags: [flask, django, wsgi, asgi, fastpath, async, tdd]
---

# Task t05: Add `async def` view execution, cached `MsgspecView`, `wrap_asgi`, and `_WsgiStartResponse.send_raw_response` to Flask and Django `FastPath`

## Objective
Enable `async def` view functions and `MsgspecView` async methods in `Flask` (`src/py/vertebra/extensions/flask/fastpath.py::FlaskFastPath`) and `Django` (`src/py/vertebra/extensions/django/fastpath.py::DjangoFastPath`), cache stateless `MsgspecView` instances in `MsgspecView.as_view`, add ASGI `dispatch_async` and `wrap_asgi` to both `FlaskFastPath` and `DjangoFastPath`, make the target WSGI callable optional in `wrap_wsgi`, and wire `_WsgiStartResponse.send_raw_response` in `dispatch_wsgi`.

## Context
- **Verification Strategy (`behavior_tdd`)**: Write failing tests in `src/py/tests/unit/extensions/test_flask.py` and `src/py/tests/unit/extensions/test_django.py` testing `async def` Flask views and `async def` Django views / `MsgspecView` async methods across `FastPath.dispatch`, `FastPath.dispatch_async`, `FastPath.dispatch_wsgi` (with `send_raw_response`), and `FastPath.wrap_asgi`.
- **Architectural Invariants**:
  - When a non-suspending `async def` view is invoked via synchronous `dispatch()` or `dispatch_wsgi()`, step the coroutine via `coro.send(None)`: if it raises `StopIteration(val)`, serialize `val` immediately with zero event loop overhead; if it yields a pending awaitable, drive it via `dispatch_async` / `wrap_asgi`.
  - In `MsgspecView.as_view(**initkwargs)`, pre-instantiate `cached_instance = cls()` once at `as_view()` time when `not initkwargs`, and inspect whether any HTTP method handler (`get`, `post`, `put`, `patch`, `delete`) is an `async def` coroutine function.

## Steps
1. **Write failing unit tests (`RED`)**:
   - In `src/py/tests/unit/extensions/test_flask.py`, add tests for:
     - `@vertebra_route` on an `async def` Flask view with path params and a `msgspec.Struct` body executed via `FlaskFastPath.dispatch`, `FlaskFastPath.dispatch_async`, `FlaskFastPath.dispatch_wsgi`, and `FlaskFastPath.wrap_asgi`.
     - `FlaskFastPath.dispatch_wsgi` calling `start_response.send_raw_response` when present on `start_response`.
   - In `src/py/tests/unit/extensions/test_django.py`, add tests for:
     - `@vertebra_view` on an `async def` Django view and a `MsgspecView` subclass with `async def post(self, request, payload: ...)` executed via `DjangoFastPath.dispatch`, `DjangoFastPath.dispatch_async`, `DjangoFastPath.dispatch_wsgi`, and `DjangoFastPath.wrap_asgi`.
     - Stateless `MsgspecView.as_view()` reusing its pre-allocated instance across requests.
2. **Implement Flask & Django `async def`, `wrap_asgi`, and `send_raw_response` (`GREEN`)**:
   - In `src/py/vertebra/extensions/flask/fastpath.py`:
     - Add `_step_coro_sync(coro)` helper for synchronous `dispatch` / `dispatch_wsgi` of non-suspending `async def` views.
     - Add `async def dispatch_async(self, scope: Any, receive: Any, send: Any, *, body: bytes | None = None) -> tuple[int, list[tuple[bytes, bytes]], bytes] | None` and `def wrap_asgi(self, fallback_app: Callable[..., Any] | None = None) -> Callable[..., Any]` using `wrap_asgi_fastpath` from `vertebra.server._fastpath`, and allow `wrap_wsgi(self, wsgi_app: Callable[..., Any] | None = None)` to default to `self._compiler._app.wsgi_app` when `wsgi_app` is `None`.
     - In `dispatch_wsgi`, strip `environ.get("SCRIPT_NAME", "")` or `mount_prefix` if needed and call `start_response.send_raw_response(resp.status, resp.headers, bytes(resp.body))` when `hasattr(start_response, "send_raw_response")` is True.
   - In `src/py/vertebra/extensions/django/fastpath.py`:
     - Update `MsgspecView.as_view` to cache `cls()` when `not initkwargs`, support `async def` HTTP method handlers, and set `__vertebra_is_async__` when any method handler is `async def`.
     - Update `_compile_single_callback` to check `inspect.iscoroutinefunction(callback) or bool(getattr(callback, "__vertebra_is_async__", False))`.
     - Add `_step_coro_sync`, `dispatch_async`, `wrap_asgi(self, fallback_app: Callable[..., Any] | None = None)`, optional `wsgi_handler: Callable[..., Any] | None = None` in `wrap_wsgi`, and `start_response.send_raw_response` support in `dispatch_wsgi`.
3. **Refactor & verify (`REFACTOR`)**:
   - Run `ruff`, `mypy`, and the Flask and Django unit test suites.

## Verification
```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_flask.py src/py/tests/unit/extensions/test_django.py -q
uv run --no-sync ruff check src/py/vertebra/extensions/flask/fastpath.py src/py/vertebra/extensions/django/fastpath.py src/py/tests/unit/extensions/test_flask.py src/py/tests/unit/extensions/test_django.py
uv run --no-sync mypy src/py/vertebra/extensions/flask/fastpath.py src/py/vertebra/extensions/django/fastpath.py
```

## Acceptance Criteria
- [ ] `Flask` and `Django` `FastPath` execute both synchronous `def` and `async def` views (including `MsgspecView` async methods) across `dispatch`, `dispatch_async`, `dispatch_wsgi`, and `wrap_asgi` (R5).
- [ ] `MsgspecView.as_view()` reuses a pre-allocated view instance when `initkwargs` is empty (R5).
- [ ] `dispatch_wsgi` in both `Flask` and `Django` invokes `start_response.send_raw_response` when supported (R5).

## Notes & Discoveries
