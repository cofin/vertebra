---
type: Guide
title: Vertebra Product and Engineering Guidelines
description: Engineering invariants, domain naming rules, zero-copy hot-path standards, and code quality conventions across Python, Rust, and TypeScript.
status: stable
tags: [guidelines, conventions, performance, naming, architecture]
sources:
  - resource: src/py/vertebra/__init__.py
  - resource: src/py/vertebra/server/_native.py
  - resource: src/py/vertebra/server/_asgi/_runner.py
  - resource: src/py/vertebra/framework/_router.py
  - resource: src/py/vertebra/utils/serializers/_json.py
  - resource: src/py/vertebra/_typing.py
  - resource: src/rs/vertebra-core/src/channel.rs
  - resource: src/rs/vertebra-http/src/lib.rs
  - resource: src/ts/vertebra-client/src/index.ts
---

# Vertebra Product and Engineering Guidelines

## 1. Domain Naming Without Aliases

Public and internal symbols across Python, Rust, and TypeScript name *what* the domain concept is. They carry no `Vertebra*` brand prefix and no implementation-mechanism suffix:

- **Canonical Domain Names**: `App`, `Router`, `RouteDef`, `HandlerInfo`, `Request`, `Response`, `Headers`, `HeadersIterator`, `RequestBatch`, `RequestBatchIterator`, `AsgiScope`, `AsgiScopeIterator`, `WsgiEnviron`, `WsgiEnvironIterator`, `AsyncRuntime`, `BatchDispatchPlan`, `AsgiRunner`, `AsgiReceive`, `AsgiSend`, `RsgiAdapter`, `WsgiAdapter`, `Bundler`, `SourceWatcher`, `SsrPool`, `BuildOrchestrator`, `DevServerProcess`, `TypeExtractor`, `ManifestGenerator`, `RequestBatcher`, `BatchPolicy`, `InFlightRequests`, `PendingRequests`, `RouteTable`, `ResponseBody`, `HttpResponse`, `JSONSerializer`, `OptionalDependency`, `Console`, `ObjectStore`, `SyncStore`, `AsyncStore`, `SQLSpecPlugin`, `AlchemyPlugin`, `BrokerPlugin`, `LitestarPlugin`, `ChannelsBackend`, `MemoryStore`, `WorkerRunner`, `LaneConfig`, `LiveState`, `LogSink`, `LogHandler`, `TraceContext`, `CgroupQuota`, `LameDuckController`, `ZPagesRegistry`, and on the TypeScript side `HttpClient`, `ClientConfig`, `ClientError`, `RealtimeChannel`, `SseClient`, `WebSocketClient`, `SessionStore`, `HotReloadClient`, `LiveStateClient`, and `TraceContext`.
- **No Backward-Compatibility Shims**: Do not reintroduce removed aliases (`VertebraApp`, `CallbackScheduler`, `LocalExchange`, `BatchCollector`, `CorrelationSlab`, `DrainPolicy`, `NativeRouter`, `VertebraBody`, `VertebraResponse`, `VertebraClient`, `VertebraStore`, `VertebraHmrClient`, `VertebraSQLSpecPlugin`, `VertebraAlchemyPlugin`, `VertebraPlugin`, `VertebraChannelsBackend`, `VertebraMemoryStore`, `MultiLaneWorkerRunner`, `VertebraSseClient`, `VertebraWebSocketClient`). `src/py/tests/unit/extensions/test_drop_in_names_and_generics.py` asserts that `VertebraPlugin` and `VertebraSQLSpecPlugin` are absent.
- **Exception Root**: [`VertebraError`](../../../src/py/vertebra/_typing.py) is the one remaining `Vertebra*` name; it is the package's base exception class.

## 2. Zero-Copy and Allocation-Minimal Hot Paths

Per-request and per-batch code paths minimize heap allocations, copies, and cross-thread wakeups:

- **Rust Shared Memory & Bridge**: Write serialized responses directly into ring slots through closure-based `SpscRing::try_send_with` / `MpmcRing::try_send_with`, and read through zero-copy `SlotView` (`SpscRing::try_recv_view`) or `MpmcRing::try_recv_with` ([`channel.rs`](../../../src/rs/vertebra-core/src/channel.rs)). Recycle `TransportArena` and `DataSlab` allocations through `BufferPool`.
- **PyO3 FFI Boundary**: Release the Python GIL with `py.detach(|| ...)` on every blocking poll (`get_batch`), batch response submission (`submit_results`, `submit_response`), RSGI file I/O, Oxc bundle build (`Bundler::build`), QuickJS SSR render (`SsrPool::render`), and native object-store calls in `_storage`.
- **Python Dispatch**: Partition each batch once into [`BatchDispatchPlan`](../../../src/py/vertebra/server/_native.py) (`sync_native`, `async_native`, `mounts`, `unmatched`), cache handler signatures with `@functools.lru_cache(maxsize=512)` on `_handler_dispatch_spec`, wake the `AsyncRuntime` event loop once per batch through `spawn_batch`/`schedule_batch`, and drive ASGI coroutines inline with [`_drive_coro`](../../../src/py/vertebra/server/_asgi/_runner.py) when `VERTEBRA_ASGI_DIRECT_SEND` is enabled; a coroutine that suspends is promoted to a task by `_resume_coro_in_task`.
- **Lazy Package Startup**: Keep [`vertebra/__init__.py`](../../../src/py/vertebra/__init__.py) free of eager submodule imports. Its PEP 562 `__getattr__` loads `vertebra.framework` exports on first attribute access, so `import vertebra` does not import the framework, server, or native extensions.

## 3. Python Module Organization

- **Python `>=3.12` Native Typing**: Use PEP 695 generic syntax (`def schedule[T]`, `class SQLSpecPlugin[ConfigT]`), PEP 604 unions (`X | Y`), and PEP 585 built-in collection types (`list`, `dict`, `tuple`, `set`). Do not add `typing_extensions` shims.
- **Value Objects**: Config, metadata, spec, and frame value objects are `msgspec.Struct(..., frozen=True, gc=False)`, adding `array_like=True` on hot-path records such as [`RouteDef`](../../../src/py/vertebra/framework/_router.py) and `HandlerInfo`.
- **JSON Serialization**: Route JSON through [`vertebra.utils.serializers`](../../../src/py/vertebra/utils/serializers/_json.py) (`encode_json`, `encode_json_bytes`, `encode_json_into`, `decode_json`, `format_json`, `JSONSerializer`) rather than calling `msgspec.json` or `json` directly in other modules.
- **No Inline `#` Comments**: Do not place inline `#` comments in Python implementation or test files. Document intent, invariants, and edge cases in Google-style docstrings.
- **Module & Class Layout**:
  1. Module docstring at the top.
  2. Top-level imports grouped by standard library, third-party (`msgspec`, `anyio`, `rich`), and first-party (`vertebra.*`), with `if TYPE_CHECKING:` blocks at module top level for type-only or optional imports.
  3. Explicit `__all__` tuple.
  4. Public classes and functions before private `_...` module-level helpers.
  5. Within classes: `__slots__` / class attributes, then `__init__`, then `@property`, then public methods, then private `_...` helper methods.

## 4. Polyglot Boundary & Git Discipline

- **Single FNV-1a Route Identity**: Route matching happens in Rust ([`Router`](../../../src/rs/vertebra-http/src/router.rs) / `matchit 0.9`), which passes a `u64` handler id to Python workers. Any change to path normalization or FNV-1a hashing must keep [`src/rs/vertebra-http/src/router.rs`](../../../src/rs/vertebra-http/src/router.rs) and [`src/py/vertebra/framework/_router.py`](../../../src/py/vertebra/framework/_router.py) synchronized.
- **Dependency Hygiene**: Keep benchmark-only packages (`duckdb`, `uvicorn`, `httptools`, `uvloop`, `granian`, `spikard`) in `[dependency-groups].bench` in [`pyproject.toml`](../../../pyproject.toml), never in `dev`.
- **Branch & PR Discipline**: Never commit or push directly to `main`. Develop on a dedicated branch with signed commits, verify with `make lint-all` and focused test targets, and deliver through a pull request.

## Related Context

- [Product Definition](product.md)
- [Tech Stack](tech-stack.md)
- [Python Module & Class Layout](../knowledge/patterns/python/module-and-class-layout.md)
- [PyO3 FFI & GIL Discipline](../knowledge/patterns/rust/ffi-and-gil-discipline.md)
