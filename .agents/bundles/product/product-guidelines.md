---
type: Guide
title: Vertebra Product and Engineering Guidelines
description: Engineering invariants, domain naming rules, deep-module seam design, zero-copy hot-path standards, and code quality conventions across Python, Rust, and TypeScript.
status: stable
tags: [guidelines, conventions, performance, naming, architecture]
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-init-py
    resource: src/py/vertebra/__init__.py
    title: "Python implementation module (src/py/vertebra/__init__.py)"
  - id: src-server-dispatch-py
    resource: src/py/vertebra/server/_dispatch.py
    title: "Python implementation module (src/py/vertebra/server/_dispatch.py)"
  - id: src-server-asgi-runner-py
    resource: src/py/vertebra/server/_asgi/_runner.py
    title: "Python implementation module (src/py/vertebra/server/_asgi/_runner.py)"
  - id: src-framework-router-py
    resource: src/py/vertebra/framework/_router.py
    title: "Python implementation module (src/py/vertebra/framework/_router.py)"
  - id: src-utils-serializers-py
    resource: src/py/vertebra/utils/serializers/__init__.py
    title: "Python implementation module (src/py/vertebra/utils/serializers/__init__.py)"
  - id: src-typing-py
    resource: src/py/vertebra/typing.py
    title: "Python implementation module (src/py/vertebra/typing.py)"
  - id: src-typing-py-2
    resource: src/py/vertebra/_typing.py
    title: "Python implementation module (src/py/vertebra/_typing.py)"
  - id: src-vertebra-core-src-channel-rs
    resource: src/rs/vertebra-core/src/channel.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/channel.rs)"
  - id: src-vertebra-http-src-lib-rs
    resource: src/rs/vertebra-http/src/lib.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/lib.rs)"
  - id: src-src-index-ts
    resource: src/ts/vertebra-client/src/index.ts
    title: "TypeScript client module (src/ts/vertebra-client/src/index.ts)"
---

# Vertebra Product and Engineering Guidelines

<!-- truth: start -->
- **Clean-Break Domain Naming (`PY-07`)**: Name symbols across Python, Rust, and TypeScript after their domain role (`App`, `Router`, `Request`, `LeaseBroker`, `SyncObjectStore`, `HttpClient`) without `Vertebra*` or `Native*` prefixes (`VertebraError`, host-framework `VertebraPlugin`/`VertebraConfig`/`VertebraAppConfig`, and CLI `VertebraEnv`/`VertebraGroup`/`VertebraExtensionGroup` are the sole exceptions) and with zero backward-compatibility `X = Y` aliases.
- **Deep Modules & Seam Classification**: Expose compact public interfaces that hide IPC, FFI, and wire-framing internals. Classify test dependencies into `In-process`, `Local-substitutable`, `Remote-owned`, and `External boundary`.
- **Zero-Copy Hot Paths & Value Objects (`PY-04`, `PY-05`)**: Use `msgspec.Struct(..., frozen=True, gc=False)` (`array_like=True` only on internal ring frames), route JSON through `vertebra.utils.serializers`, release the GIL via `py.detach(|| ...)`, and hydrate DTOs through C-API `msgspec_bridge.rs`.
- **Python Layout & Zero Inline Comments (`PY-01`, `PY-02`, `PY-06`, `PY-08`)**: No inline `#` comments in Python files (use Google-style docstrings); top-level grouped imports; public `vertebra.typing` facade; cached `vertebra.utils.module_loader`; PEP 810 `__lazy_modules__` + PEP 562 `__getattr__`.
- **Public-Seam Vertical-Slice TDD & Cold-Reader Docs**: Ship vertical slices with observable public-seam tests in canonical test files; keep `docs/` conceptual (`4–6` node Mermaid diagrams, `pymdownx.tabbed` install blocks, zero contributor `make` commands in user docs).
- **Polyglot Router Parity & Git Discipline (`HTTP-10`)**: Keep FNV-1a route hashing identical between Rust (`routing.rs`) and Python (`_router.py`), and land all changes on dedicated branches via signed conventional commits (`commit.gpgsign=true`).
<!-- truth: end -->

## 1. Domain Naming Without Aliases

Public and internal symbols across Python, Rust, and TypeScript name *what* the domain concept is, without brand prefixes or implementation suffixes:

- **Domain-Role Names**: Core framework (`App`, `Controller`, `Router`, `Route`, `Procedure`), server/FFI (`Request`, `Response`, `AsgiScope`, `RsgiAdapter`, `WsgiAdapter`, `EventLoopPool`), frontend (`Bundler`, `SourceWatcher`, `SSRPool`, `BuildOrchestrator`, `HttpClient`), storage/data (`SyncObjectStore`, `AsyncObjectStore`, `LeaseBroker`, `ArrowCursor`, `SyncPostgreSQLDialect`, `SyncCloudSQLDialer`), and platform (`WorkerRunner`, `LaneConfig`, `LogSink`, `TraceContext`, `ZPagesRegistry`).
- **Sync/Async Split Convention**: Classes split across execution modes use explicit leading `Sync*` and `Async*` prefixes with un-suffixed method names (`put`, `get`, `execute`, `render`, `connect`) and zero mixed `*_async` methods. The only exceptions are upstream-imposed SQLSpec naming (`<Engine>SyncConfig`/`<Engine>AsyncConfig`, `<Engine>SyncDriver`/`<Engine>AsyncDriver`, `SQLSpecSyncProvider`/`SQLSpecAsyncProvider`) and upstream Advanced Alchemy / SQLSpec `ObjectStoreBackend` protocol adapters.
- **No Compatibility Shims**: Do not reintroduce removed `Vertebra*`, `Native*`, or host-prefixed aliases (`VertebraApp`, `NativeRouter`, `VertebraClient`, `LitestarPlugin`, `PostgresVtbDialect`, `NativeLeaseBroker`, etc.). [`test_drop_in_names_and_generics.py`](../../../src/py/tests/unit/extensions/test_drop_in_names_and_generics.py) and [`tools/scripts/review_setup.py`](../../../tools/scripts/review_setup.py) (`PY-07`) enforce this rule.
- **Permitted `Vertebra*` Exceptions**: [`VertebraError`](../../../src/py/vertebra/_typing.py)[^src-typing-py-2] (root exception), `VertebraPlugin` / `VertebraConfig` / `VertebraAppConfig` (inside host-framework extension packages `vertebra.extensions.{litestar,starlette,fastapi,flask,django}`), and `VertebraEnv` / `VertebraGroup` / `VertebraExtensionGroup` (for `vtb` CLI extension groups).

## 2. Zero-Copy and Allocation-Minimal Hot Paths

Per-request and per-batch paths minimize heap allocations, copies, and cross-thread wakeups:

- **Rust Shared Memory & Bridge**: Write serialized responses directly into ring slots via closure-based `SpscRing::try_send_with` / `MpmcRing::try_send_with`, read via zero-copy `SlotView` / `MpmcRing::try_recv_with` ([`channel.rs`](../../../src/rs/vertebra-core/src/channel.rs)[^src-vertebra-core-src-channel-rs]), and recycle `TransportArena` / `DataSlab` allocations through `BufferPool`.
- **PyO3 FFI & C-API `msgspec` Bridge**: Release the GIL with `py.detach(|| ...)` around blocking polls, batch response submits on full rings, RSGI file I/O, Oxc builds, QuickJS renders, and `_storage` calls. Decode request bodies and hydrate rows over borrowed `Py_buffer` slices via `Request.decode_msgspec`, `WsgiInput.decode_msgspec`, and `hydrate_rows_vectorcall` ([`msgspec_bridge.rs`](../../../src/rs/vertebra-py/src/msgspec_bridge.rs)).
- **Python Dispatch**: Partition each batch once into [`BatchDispatchPlan`](../../../src/py/vertebra/server/_dispatch.py)[^src-server-dispatch-py], pre-compile handler signatures at startup (`compile_handler_table`), run default sync routes inline in one `submit_batch_raw` flush, wake `EventLoopPool` once per shard, and drive ASGI/RSGI/WSGI batches in Rust (`drive_asgi_batch`, `drive_rsgi_batch`, `drive_wsgi_batch`).
- **Lazy Package Startup**: Keep [`vertebra/__init__.py`](../../../src/py/vertebra/__init__.py)[^src-init-py] and [`vertebra/framework/__init__.py`](../../../src/py/vertebra/framework/__init__.py) free of eager submodule imports by pairing PEP 810 `__lazy_modules__` with PEP 562 `__getattr__`.

## 3. Python Module Organization

- **Native Typing (`>=3.12`)**: Use PEP 695 generics (`def schedule[T]`, `class SQLSpecPlugin[ConfigT]`), PEP 604 unions (`X | Y`), and PEP 585 built-in collections (`list`, `dict`, `tuple`, `set`). Avoid `typing_extensions` shims.
- **Value Objects & Base Structs**: Define config, metadata, spec, and frame objects as `msgspec.Struct(..., frozen=True, gc=False)` or subclasses of [`BaseStruct` / `FrozenBaseStruct`](../../../src/py/vertebra/typing.py)[^src-typing-py]. Use `cache_hash=True` on hashable keys, `forbid_unknown_fields=True` on inbound request schemas, and `array_like=True` strictly on internal wire/ring frames (`ChannelMessage`, `StateDeltaFrame`, `TaskEnvelope`) — never on HTTP/SSE/JSON response models.
- **JSON Serialization**: Route JSON and zero-copy arena framing through [`vertebra.utils.serializers`](../../../src/py/vertebra/utils/serializers/__init__.py)[^src-utils-serializers-py] rather than calling `msgspec.json` or `json` directly.
- **No Inline `#` Comments**: Never add inline `#` comments in Python implementation or test files; document intent, invariants, and edge cases in Google-style docstrings.
- **Module & Class Layout**:
  1. Module docstring.
  2. Top-level imports grouped by stdlib, third-party, and first-party (`if TYPE_CHECKING:` at top level for type-only or optional imports).
  3. Explicit `__all__` tuple.
  4. Public classes and functions before private `_...` helpers.
  5. Within classes: `__slots__` / class attributes, `__init__`, `@property`, public methods, then private `_...` helpers.

## 4. Deep Modules, Seam Classification & Documentation Standards

- **Deep Modules**: Keep public interfaces narrow (`App`, `Router`, `SyncObjectStore`, `LeaseBroker`, `ChannelBus`, `HttpClient`) so wire protocols, ring layouts, and C-API slot offsets stay encapsulated.
- **Dependency Seam Classification**:
  - `In-process`: Pure computation, `msgspec` codecs, route compilation, and wire parsers — test directly without doubles.
  - `Local-substitutable`: Shared-memory rings, SQLite, in-memory object stores (`MemoryConfig`), and local emulators — test against real local backends.
  - `Remote-owned`: External cloud control planes and database servers — isolate behind adapter/driver protocols (`SyncAdapterConfig`, `AsyncAdapterConfig`, `QueueBackend`).
  - `External boundary`: Third-party webhook/HTTP endpoints — mock only at the outer transport edge and assert observable bytes, headers, and state transitions.
- **User Documentation (`docs/`)**:
  - Write for a cold reader without contributor commands (`make dev`, `make build-pgo`, `make test`).
  - Keep `docs/` Mermaid diagrams conceptual (`4–6` nodes); put detailed implementation diagrams in `.agents/bundles/knowledge/architecture/`.
  - Format installation blocks with linked `pymdownx.tabbed` tabs (`=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"`).

## 5. Polyglot Boundary & Git Discipline

- **Single FNV-1a Route Identity**: Rust ([`routing.rs`](../../../src/rs/vertebra-http/src/routing.rs)) matches routes and passes a `u64` handler ID to Python workers. Any path normalization or FNV-1a change must keep Rust and [`_router.py`](../../../src/py/vertebra/framework/_router.py)[^src-framework-router-py] identical.
- **Dependency Hygiene**: Keep benchmark-only packages (`duckdb`, `uvicorn`, `httptools`, `uvloop`, `granian`, `spikard`) in `[dependency-groups].bench` and documentation tools in `[dependency-groups].doc` in [`pyproject.toml`](../../../pyproject.toml), never in `dev`.
- **Branch & PR Discipline**: Never commit or push directly to `main`. Use dedicated branches with signed commits, verify with `make lint-all` and focused test targets, and submit via pull request.

## Related Context

- [Product Definition](product.md)
- [Tech Stack](tech-stack.md)
- [Python Module & Class Layout](../knowledge/patterns/python/module-and-class-layout.md)
- [PyO3 FFI & GIL Discipline](../knowledge/patterns/rust/ffi-and-gil-discipline.md)

Provenance sources: [^src-server-asgi-runner-py], [^src-vertebra-http-src-lib-rs], [^src-src-index-ts].

[^src-init-py]: `src/py/vertebra/__init__.py` — Python implementation module (src/py/vertebra/__init__.py).
[^src-server-dispatch-py]: `src/py/vertebra/server/_dispatch.py` — Python implementation module (src/py/vertebra/server/_dispatch.py).
[^src-server-asgi-runner-py]: `src/py/vertebra/server/_asgi/_runner.py` — Python implementation module (src/py/vertebra/server/_asgi/_runner.py).
[^src-framework-router-py]: `src/py/vertebra/framework/_router.py` — Python implementation module (src/py/vertebra/framework/_router.py).
[^src-utils-serializers-py]: `src/py/vertebra/utils/serializers/__init__.py` — Python implementation module (src/py/vertebra/utils/serializers/__init__.py).
[^src-typing-py]: `src/py/vertebra/typing.py` — Python implementation module (src/py/vertebra/typing.py).
[^src-typing-py-2]: `src/py/vertebra/_typing.py` — Python implementation module (src/py/vertebra/_typing.py).
[^src-vertebra-core-src-channel-rs]: `src/rs/vertebra-core/src/channel.rs` — Rust implementation module (src/rs/vertebra-core/src/channel.rs).
[^src-vertebra-http-src-lib-rs]: `src/rs/vertebra-http/src/lib.rs` — Rust implementation module (src/rs/vertebra-http/src/lib.rs).
[^src-src-index-ts]: `src/ts/vertebra-client/src/index.ts` — TypeScript client module (src/ts/vertebra-client/src/index.ts).
