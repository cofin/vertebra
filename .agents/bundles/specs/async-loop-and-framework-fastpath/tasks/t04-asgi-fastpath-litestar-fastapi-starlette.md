---
type: Task
id: async-loop-and-framework-fastpath:t04
title: "Unify zero-closure wrap_asgi, AsgiSend.send_raw_response, lazy Request, and inline sync execution across Litestar, FastAPI, and Starlette FastPath"
state: done
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - async-loop-and-framework-fastpath:t02
files:
  - src/py/vertebra/server/_fastpath.py
  - src/py/vertebra/extensions/litestar/fastpath.py
  - src/py/vertebra/extensions/fastapi/fastpath.py
  - src/py/vertebra/extensions/fastapi/dependencies.py
  - src/py/vertebra/extensions/starlette/fastpath.py
tests:
  - src/py/tests/unit/server/test_litestar_direct_fastpath.py
  - src/py/tests/unit/extensions/test_fastapi_plugin.py
  - src/py/tests/unit/extensions/test_starlette_plugin.py
plan_revision: 2
plan_commit: 34429ff8c50d4622d0cacbd1288525f8a1211dc8
state_revision: 2
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: 2026-11-02T00:49:01Z
last_verified_commit: 3e7a775
verification_evidence: "uv run --no-sync pytest src/py/tests/unit/server/test_litestar_direct_fastpath.py src/py/tests/unit/extensions/test_fastapi_plugin.py src/py/tests/unit/extensions/test_starlette_plugin.py -q -> 16 passed; ruff & mypy -> exit 0"
created_at: 2026-11-01T23:48:00Z
updated_at: 2026-11-02T00:49:15Z
commit: null
tags: [litestar, fastapi, starlette, asgi, fastpath, zero-copy, tdd]
---

# Task t04: Unify zero-closure `wrap_asgi`, `send_raw_response`, lazy `Request`, and inline sync execution across Litestar, FastAPI, and Starlette `FastPath`

## Objective
Create `src/py/vertebra/server/_fastpath.py` with shared zero-closure `wrap_asgi_fastpath`, zero-copy `AsgiScope.body_bytes()` body extraction (`extract_scope_body_fast`), and single-call `send_asgi_direct` (`send.send_raw_response`), and wire it across `Litestar` (`LitestarFastPathBypass`, including adding `wrap_asgi`), `FastAPI` (`FastAPIFastPath`), and `Starlette` (`StarletteFastPath`)—along with lazy `starlette.requests.Request` instantiation and default inline sync execution (`sync_to_thread: bool = False`) in `FastAPI` and `Starlette`.

## Context
- **Verification Strategy (`behavior_tdd`)**: Write failing tests in `src/py/tests/unit/server/test_litestar_direct_fastpath.py`, `src/py/tests/unit/extensions/test_fastapi.py`, and `src/py/tests/unit/extensions/test_starlette.py` verifying that `wrap_asgi` uses `send.send_raw_response` when `send` is a native `AsgiProtocol` / `AsgiSend` (or custom callable exposing `send_raw_response`), reads `AsgiScope.body_bytes()` without calling `await receive()`, skips instantiating `starlette.requests.Request` in `FastAPI` when neither the endpoint nor `Depends()` requests `Request`, and executes sync endpoints inline when `sync_to_thread=False`.
- **Architectural Invariants**:
  - Preserve full replay fallback semantics (`_buffering_receive` / `_replay_receive`) when a route reads from `receive()` on a plain `dict` scope and subsequently returns `None` to fall back to `fallback_app`.
  - Strip `root_path` / `mount_prefix` from `scope["path"]` when matching routes in `FastPath.dispatch` so mounted sub-apps match relative paths cleanly.

## Steps
1. **Write failing unit tests (`RED`)**:
   - In `src/py/tests/unit/server/test_litestar_direct_fastpath.py`, add a test verifying `LitestarFastPathBypass.wrap_asgi` invokes `send.send_raw_response(status, headers, body)` directly when available on `send` and strips `root_path` prefix when matching mounted routes.
   - In `src/py/tests/unit/extensions/test_fastapi.py`, add tests verifying:
     - `CompiledRoute.needs_request` is `False` when neither the endpoint nor `CompiledDependantPlan` accepts a `Request` parameter, and `dispatch_handler_direct` does not instantiate `starlette.requests.Request`.
     - Synchronous `def` FastAPI endpoints execute inline on the calling thread by default (`sync_to_thread=False`) and offload to `anyio.to_thread.run_sync` when `__vertebra_sync_to_thread__ = True`.
     - `FastAPIFastPath.wrap_asgi` calls `send.send_raw_response` when available.
   - In `src/py/tests/unit/extensions/test_starlette.py`, add tests verifying `send.send_raw_response` usage and `sync_to_thread=False` inline sync execution.
2. **Implement `_fastpath.py` and update `Litestar`, `FastAPI`, and `Starlette` `FastPath` (`GREEN`)**:
   - Create `src/py/vertebra/server/_fastpath.py` with:
     - `resolve_relative_path(scope: Any, mount_prefix: str = "") -> str` (stripping `mount_prefix` or `scope["root_path"]` prefix from `scope["path"]`).
     - `async def extract_scope_body_fast(scope: Any, receive: Any, body: bytes | None = None, *, method: str = "GET") -> bytes` (returning `body` if provided, `b""` on bodyless methods, or calling `scope.body_bytes()` / `receive.__self__.body_bytes()` synchronously when `scope` is a native `AsgiScope` or `receive` is a bound `AsgiProtocol.receive` before falling back to `await receive()`).
     - `async def send_asgi_direct(send: Any, status_code: int, encoded_headers: list[tuple[bytes, bytes]], raw_body: bytes) -> None` (checking `getattr(send, "send_raw_response", None) or getattr(getattr(send, "__self__", None), "send_raw_response", None)` and awaiting/calling it in 1 C-API call, else awaiting the 2 standard ASGI dicts).
     - `wrap_asgi_fastpath(bypass: Any, fallback_app: Callable[..., Any]) -> Callable[..., Any]` (fast-pathing `AsgiScope` requests without closure allocations when body is pre-buffered in `AsgiScope`, while retaining replay buffering for plain dict scopes).
   - Update `src/py/vertebra/extensions/litestar/fastpath.py` (adding `def wrap_asgi(self, fallback_app: Callable[..., Any]) -> Callable[..., Any]` to `LitestarFastPathBypass`), `src/py/vertebra/extensions/fastapi/fastpath.py`, `src/py/vertebra/extensions/fastapi/dependencies.py` (adding `needs_request: bool` to `CompiledDependantPlan`), and `src/py/vertebra/extensions/starlette/fastpath.py` to use `wrap_asgi_fastpath`, `resolve_relative_path`, `extract_scope_body_fast`, `send_asgi_direct`, lazy `Request` construction, and `sync_to_thread: bool = False`.
3. **Refactor & verify (`REFACTOR`)**:
   - Run `ruff`, `mypy`, and all three test suites.

## Verification
```bash
uv run --no-sync pytest src/py/tests/unit/server/test_litestar_direct_fastpath.py src/py/tests/unit/extensions/test_fastapi.py src/py/tests/unit/extensions/test_starlette.py -q
uv run --no-sync ruff check src/py/vertebra/server/_fastpath.py src/py/vertebra/extensions/litestar/fastpath.py src/py/vertebra/extensions/fastapi/ src/py/vertebra/extensions/starlette/fastpath.py
uv run --no-sync mypy src/py/vertebra/server/_fastpath.py src/py/vertebra/extensions/litestar/fastpath.py src/py/vertebra/extensions/fastapi/ src/py/vertebra/extensions/starlette/fastpath.py
```

## Acceptance Criteria
- [x] `Litestar`, `FastAPI`, and `Starlette` `FastPath.wrap_asgi` emit responses via `send.send_raw_response` in 1 C-API call when supported and preserve replay fallback on unbuffered `dict` scopes (R4).
- [x] `FastAPI` `dispatch_handler_direct` skips constructing `starlette.requests.Request` when `needs_request` is `False` (R4).
- [x] Synchronous `def` endpoints in `FastAPI` and `Starlette` `FastPath` execute inline when `sync_to_thread=False` (R4).

## Notes & Discoveries
- Created `src/py/vertebra/server/_fastpath.py` with `resolve_relative_path`, `extract_scope_body_fast`, `send_asgi_direct`, `wrap_asgi_fastpath`, and `attach_framework_fastpath`.
- Wired `wrap_asgi_fastpath`, `resolve_relative_path`, and `extract_scope_body_fast` across `Litestar`, `FastAPI`, and `Starlette` `FastPath`, added `CompiledRoute.needs_request` in `FastAPI`, and defaulted sync fastpath endpoints and dependencies to inline execution (`sync_to_thread: bool = False`).
