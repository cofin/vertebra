---
type: Guide
title: Vertebra Product and Engineering Guidelines
description: Engineering invariants, clean-break domain naming rules, zero-copy hot-path standards, and code quality conventions across Python, Rust, and TypeScript.
status: stable
tags: [guidelines, conventions, performance, naming, architecture]
sources:
  - resource: src/py/vertebra/__init__.py
  - resource: src/py/vertebra/server/_native.py
  - resource: src/rs/vertebra-core/src/lib.rs
  - resource: src/rs/vertebra-http/src/lib.rs
  - resource: src/ts/vertebra-client/src/index.ts
---

# Vertebra Product and Engineering Guidelines

## 1. Clean-Break Domain Naming (Zero Legacy Aliases)

Public and internal symbols across Python, Rust, and TypeScript must express *what* the domain concept is rather than *how* it is implemented or prefixed with redundant brand/mechanism tokens (`commit 6dc6be4`):

- **Canonical Domain Names**: Use `App`, `Router`, `RouteDef`, `HandlerInfo`, `Request`, `Response`, `Headers`, `HeadersIterator`, `RequestBatch`, `RequestBatchIterator`, `AsgiScope`, `AsgiScopeIterator`, `WsgiEnviron`, `WsgiEnvironIterator`, `AsyncRuntime`, `BatchDispatchPlan`, `AsgiRunner`, `AsgiReceive`, `AsgiSend`, `WsgiAdapter`, `Bundler`, `SourceWatcher`, `SsrPool`, `BuildOrchestrator`, `DevServerProcess`, `TypeExtractor`, `ManifestGenerator`, `RequestBatcher`, `BatchPolicy`, `InFlightRequests`, `RouteTable`, `JSONSerializer`, `OptionalDependency`, `Console`, `HttpClient`, `ClientConfig`, `RealtimeChannel`, `SessionStore`, and `HotReloadClient`.
- **No Backward-Compatibility Shims**: Never reintroduce deprecated brand-prefixed or implementation-leaking aliases (`VertebraApp`, `CallbackScheduler`, `LocalExchange`, `BatchCollector`, `CorrelationSlab`, `DrainPolicy`, `NativeRouter`, `VertebraClient`, `VertebraStore`, `VertebraHmrClient`).

## 2. Zero-Copy and Allocation-Minimal Hot Paths

Every per-request and per-batch code path must minimize heap allocations, copies, and cross-thread wakeups:

- **Rust Shared Memory & Bridge**: Write serialized responses directly into ring buffer slots via closure-based [`SpscRing::try_send_with`](../../../src/rs/vertebra-core/src/channel.rs) and read via zero-copy [`SlotView`](../../../src/rs/vertebra-core/src/channel.rs) (`SpscRing::try_recv_view`) or [`MpmcRing::try_recv_with`](../../../src/rs/vertebra-core/src/ipc.rs). Recycle `TransportArena` and `DataSlab` allocations through `BufferPool`.
- **PyO3 FFI Boundary**: Release the Python GIL via `py.detach(|| ...)` on every blocking poll (`get_batch`), batch response submission (`submit_results`, `submit_response`), Oxc bundle build (`Bundler::build`), and QuickJS SSR render (`SsrPool::render`).
- **Python Dispatch**: Partition batches once into [`BatchDispatchPlan`](../../../src/py/vertebra/server/_native.py) (`sync_native`, `async_native`, `mounts`, `unmatched`), cache handler signatures via `@functools.lru_cache(maxsize=512)` in `_handler_dispatch_spec`, wake the [`AsyncRuntime`](../../../src/py/vertebra/server/_native.py) event loop once per batch via `spawn_batch`/`schedule_batch`, and drive ASGI coroutines directly via `_drive_coro` + `_CoroResumer` when `VERTEBRA_ASGI_DIRECT_SEND` is enabled.
- **Lazy Package Startup**: Keep [`vertebra/__init__.py`](../../../src/py/vertebra/__init__.py) free of eager submodule imports using PEP 562 `__getattr__` so `import vertebra` stays under `7 ms` in-process.

## 3. Clean Python Module Organization

- **Python `>=3.12` Native Typing**: Use PEP 695 generic syntax (`def schedule[T]`), PEP 604 unions (`X | Y`), and PEP 585 built-in collection types (`list`, `dict`, `tuple`, `set`). Never add `typing_extensions` shims.
- **Zero Inline `#` Comments**: Never place inline `#` comments in Python implementation or test files. Document intent, invariants, and edge cases exclusively in Google-style docstrings.
- **Module & Class Layout**:
  1. Module docstring at the top.
  2. Top-level imports grouped by standard library, third-party (`msgspec`, `anyio`, `rich`), and first-party (`vertebra.*`), with `if TYPE_CHECKING:` blocks at module top-level for type-only or optional imports.
  3. Explicit `__all__` tuple.
  4. Public classes and functions defined before private `_...` module-level helpers.
  5. Within classes: `__slots__` / class attributes $\rightarrow$ `__init__` $\rightarrow$ `@property` $\rightarrow$ public methods $\rightarrow$ private `_...` helper methods strictly below public methods.

## 4. Polyglot Boundary & Git Discipline

- **Single FNV-1a Route Identity**: Route matching happens in Rust ([`Router`](../../../src/rs/vertebra-http/src/router.rs) / `matchit 0.9`), which passes a `u64` `handler_id` to Python workers. Any change to path normalization or FNV-1a hashing must keep [`src/rs/vertebra-http/src/router.rs`](../../../src/rs/vertebra-http/src/router.rs) and [`src/py/vertebra/framework/_router.py`](../../../src/py/vertebra/framework/_router.py) synchronized.
- **Dependency Hygiene**: Keep benchmark-only packages (`duckdb`, `granian`, `spikard`, `uvicorn`) in `[dependency-groups].bench` in [`pyproject.toml`](../../../pyproject.toml), never in `dev`.
- **Branch & PR Discipline**: Never commit or push directly to `main`. All changes must be developed on a dedicated branch with signed commits, verified with `make lint-all` and focused test targets, and delivered via pull request.

## Related Context

- [Product Definition](product.md)
- [Tech Stack](tech-stack.md)
- [Python Module & Class Layout](../knowledge/patterns/python/module-and-class-layout.md)
- [PyO3 FFI & GIL Discipline](../knowledge/patterns/rust/ffi-and-gil-discipline.md)
