# Historical Decisions & Archived PRD Summary

> This document captures key architectural decisions from completed/superseded PRDs.
> Reference only - not for new work.

## Foundation PRDs (Complete)

### PRD-0: Foundation
Build system configured, workspaces created, documentation structure established.

### PRD-1: Rust IPC Core
Shared memory, SPSC/MPMC channels, buffer pools, sync primitives implemented.

### PRD-2: Python Bindings & Type Bridge
- `@procedure` decorator
- Type IR (Intermediate Representation)
- Type extractor
- Serializer
- TypeScript generator

### PRD-3: Bun Server
HTTP/1.1, HTTP/2, WebSocket, ASGI bridge, worker manager complete.

### PRD-4: Bun Drivers (SUPERSEDED)
**Why superseded**: Original socket-based driver architecture replaced by shared-memory broker. SQLSpec integration now handles database access. Vertebra's value is shared pooling, not raw driver performance.

---

## Key Architectural Decisions

### Shared Memory Over Sockets
- Data plane uses shared memory exclusively (no TCP/sockets)
- Control plane can use Protobuf/MCP

### Broker-Backed Database Access
- Bun broker owns all connection pools
- Python accesses via shared-memory C ABI ring buffers
- Custom SQLAlchemy dialects (`postgresql+vertebra`) route to broker

### Native Litestar Integration
- `PipelineCompiler` uses `exec` to generate Python functions (faster than `functools.partial`)
- `matchit` (Rust) for high-performance routing with ASGI fallback
- `VertebraRequest` bypasses `ASGIConnection` overhead

### Distribution Strategy
- Rust `NativeSupervisor` replaces Python `subprocess` for signal handling
- `python-build-standalone` for portable runtimes (not `pyembed` - incompatible with pyo3 0.27)
- Template engine uses simple string replacement (no jinja2 dependency)

---

## Consolidated Knowledge from Archived Specs

### SQLSpec Extensions
- **Adapter-First Structure**: Mirror `sqlspec/adapters/<dialect>/` inside `vertebra/extensions/sqlspec/<dialect>/`
- **Result Mapping**: `VertebraAsyncConnection.fetchrow` returns tuples; use `_collect_rows` for dict mapping
- **Async Context Awaiting**: `async with (await config.provide_connection()) as conn:` pattern
- **MySQL DDL**: No `IF NOT EXISTS` for indexes; check `information_schema.statistics`
- **SQLite Timestamps**: Use `REAL` Julian Day numbers for timezone-compatible timestamps

### Supervisor Signal Race
- **Dual Reaping Race**: Python's `subprocess.Popen.poll()` competes with Rust's `waitpid`
- **Solution**: Python actively handles exits detected by `poll()` as fallback

### Native Litestar
- **Route Reflection**: Traverse `app.routes` and `HTTPRoute` objects
- **Dynamic Compilation**: `exec` for hot path (removes Python stack frames)
- **Bun FFI Strings**: Requires manual null-termination for `ptr` type

### Distribution & Packaging
- **PyO3 GIL Release**: Use `py.detach` (not deprecated `py.allow_threads`)
- **CLI Composition**: Split large `click` CLI into modules
- **Bun.build Quirks**: Native builds ignore `outfile`, require post-processing move
- **Windows**: Job Object support needed for child process cleanup (placeholder exists)

### Elite DX & Native Bridging
- **NativeState**: `msgspec.Struct` with `__setattr__` hook for auto-broadcasting
- **Streaming Detection**: Extractor recognizes `AsyncGenerator` for `StreamType` mapping
- **Double Serialization**: `StreamingResponse` expects objects; `NativeState` yields copies

### Database & ORM Integration
- **Dialect Naming**: `postgresql+vertebra` (async), `postgresql+vertebra_sync` (sync)
- **Type-Safe Serialization**: `msgspec` hooks for 30+ non-primitive types
- **Plugin Layering**: `VertebraPlugin` as foundation, ORM plugins inherit

---

## Superseded Approaches (Do Not Use)

| Approach | Why Superseded | Replacement |
|----------|---------------|-------------|
| Socket-based drivers | Performance overhead | Shared-memory broker |
| Direct `vertebra.connect()` API | Architecture mismatch | Broker-backed SQLSpec adapters |
| `pyembed` crate | Incompatible with pyo3 0.27 | `python-build-standalone` + `pyapp` |
| CFFI bindings | Holds GIL during blocking calls | PyO3 with `allow_threads()` |
| `functools.partial` for hot paths | Stack frame overhead | `exec`-generated functions |

---

## Completed Work Reference

| Spec | Commit/Status | Notes |
|------|---------------|-------|
| eventfd-async-transport | f354dbd | Async ring buffer with eventfd notification |
| test-deadlock-fix | Complete | SHM cleanup patterns established |
| bun-broker-pool-manager | Merged | `BasePoolAdapter` in broker/base-pool.ts |
| sqlalchemy-integration | Merged | Into orm-extensions |

---

## Contact Points

- **Original specs location**: Was in `specs/archive/` (now removed)
- **Knowledge files consolidated**: Into `patterns.md` and this document
