# Project Patterns

> Consolidated learnings and patterns from all tracks.
> This file is the single source of truth for project conventions.

## Code Conventions

### Naming
- **Pool**: Reserved for broker/database pools (`BasePoolAdapter`, SQL pools, Redis pools)
- **IpcConnectionPool**: ASGI worker IPC connection pooling (`worker-connections.ts`)
- **ProcessManager**: Python worker process supervision (`process-manager.ts`)
- **Public SQLSpec APIs**: Use the `Vertebra*` prefix (configs/drivers/proxies)
- **Internal broker IPC**: Keep the `Broker*` prefix (`BrokerClient`, `BrokerClientConfig`)

### Imports
- **PyO3 vs CFFI**: Prefer PyO3 with `allow_threads()` over CFFI (CFFI holds GIL during blocking calls)
- **Native imports**: Import from `vertebra._vertebra` for Rust bindings

### Error Handling
- Use `MissingDependencyError` from `vertebra.exceptions` for optional dependencies
- Use `dependency_flag()` from `vertebra.typing` for feature detection

## Architecture Patterns

### IPC Patterns
- **MPMC Control Rings**: Use Multi-Producer Multi-Consumer rings for the "control" channel - allows multiple Python workers to register with a single Broker thread without lock contention
- **Opcode-based Dispatch**: Use integer opcodes (matched between Rust/TS/Python) for high-frequency operations (hashing, signing) - avoids JSON/MsgPack serialization overhead for method names
- **Eventfd over Polling**: Linux uses eventfd for zero-overhead async notification; macOS/Windows use polling fallback
- **Buffer Pools**: Pre-allocated buffers eliminate allocation churn (target: <1MB/s vs 650MB/s without)

### Data Loading
- **Lazy User Hydration**: `LazyUserProxy` pattern - `request.user` is available immediately with JWT claims, database fetch deferred until missing attribute accessed

### Serialization Contexts
| Context | Codec |
|---------|-------|
| Python internal | msgspec |
| Driver data plane (small) | MessagePack |
| Driver data plane (large) | Arrow IPC |
| MCP/gRPC control plane | Protobuf |
| Rust internal IPC | bincode |

## Gotchas & Warnings

### Shared Memory
- **Cleanup Required**: Named shared memory regions must be explicitly unlinked - use `ShmMpmcRing.unlink` to prevent "File exists" errors on server restarts
- **Stale Objects**: Clean `/dev/shm/test-* /dev/shm/*vertebra*` before test runs if experiencing hangs
- **FD Leaks**: Ensure `close()` is called on transports; add `__del__` cleanup

### Bun FFI
- When adding new Rust functions to `vertebra-broker`, rebuild the Rust library (`make rs-build`) **and** ensure the Bun FFI definition in `shm.ts` matches exactly - runtime symbol lookup errors otherwise

### Testing
- **Test Timeouts**: Integration tests with `subprocess` and network calls are prone to timeouts - ensure correct `PYTHONPATH`, `VERTEBRA_CONFIG` environment variables and absolute paths
- **Pytest Markers**: Use `@pytest.mark.anyio` for async tests (not `pytest-asyncio` markers) - anyio is the configured backend
- **Non-unique SHM Names**: Use uuid-based unique naming for shared memory in tests to avoid collisions

### Dependencies
- Adding Bun dependencies (like `jose`) requires ensuring the test environment (CI or local `make test`) installs them

## Testing Patterns

### Shared Memory Test Cleanup
```python
@pytest.fixture(autouse=True)
async def cleanup_shm():
    """Clean up shared memory after each test."""
    yield
    # Cleanup code
```

### UUID-based Unique Naming
```python
import uuid
shm_name = f"test-{uuid.uuid4().hex[:8]}"
```

### Context Managers for Resource Tracking
```python
async with create_channel_pair() as (tx, rx):
    # Use channels
# Automatic cleanup on exit
```

## Context for AI Assistants

### Key Architecture Points
- **Python is always the entry point**: Rust and Bun are acceleration layers
- **No sockets in data plane**: Shared memory only for broker communication
- **Bun-only drivers**: Raise explicit errors under Node

### Recovery Patterns
- Clean stale shared memory: `rm -rf /dev/shm/test-* /dev/shm/*vertebra*`
- Verify tests pass: `make test`
- Rebuild PyO3 bindings after Rust changes: `cd src/rs/vertebra-py && maturin develop`

### Mitigation Strategies (from Production Readiness)
- If broker integration is blocked: Implement in smaller slices (internal providers first)
- If SQLSpec APIs shift: Update adapter to latest stable API, don't drop support
- If Bun streaming shows instability: Fix IPC/backpressure issues, add diagnostics
- If Protobuf IPC unsupported: Keep MessagePack for MVP, track Protobuf post-MVP

## Decisions Made

### Security
- **Bcrypt Default**: Switched to `bcrypt` for password hashing in the Broker (standard, widely supported) though Argon2 is also available
- **Symmetric JWT**: Added support for symmetric key JWT verification for simple shared-secret setups alongside asymmetric (JWKS) flows

### Performance Targets
| Metric | Target |
|--------|--------|
| IPC latency | <100μs |
| Allocation rate | <1MB/s |
| Throughput | >50K req/s |

## Reusable Components

- `SecurityClient` (Python): Standalone client for Broker security functions via IPC
- `AuthValidator` (TS): Reusable JWT/IAP token validation class
- `LazyUserProxy` (Python): Generic proxy for lazy-loading models
- `BasePoolAdapter` (TS): Abstract class for all pool types in broker
- `PipelineCompiler` (Python): Adaptable for other frameworks by changing reflection logic
- `VertebraRequest` (Python): High-performance request wrapper with lazy-loaded data
- `ShmRouter` (Rust): FFI-friendly wrapper around `matchit`
- `VertebraCodec` (Python): MessagePack/JSON codec supporting 30+ Python types
- `NativeState` (Python): Base class for synchronizable state (`msgspec.Struct` with `__setattr__` hook)
- `StreamingResponse` (Python): Generic wrapper for `AsyncIterable` to NDJSON

## SQLSpec Integration Patterns

### Adapter Structure
Mirror `sqlspec/adapters/<dialect>/` inside `vertebra/extensions/sqlspec/<dialect>/` for logical consistency.

### Result Handling
- `VertebraAsyncConnection.fetchrow` returns tuples
- Use `_collect_rows` for dict mapping or `Driver.execute` with `result.one_or_none()`/`result.all()`

### Async Context Pattern
```python
# Correct: await the provider, then use async with
async with (await self.config.provide_connection()) as conn:
    ...
```

### Database-Specific Notes
- **MySQL**: No `IF NOT EXISTS` for indexes; check `information_schema.statistics`
- **SQLite**: Use `REAL` Julian Day numbers for timezone-compatible timestamps
- **MySQL DATETIME**: Timezone-naive; store UTC as naive, handle in Python layer

## Native Integration Patterns

### Dynamic Compilation
Use `exec` to generate Python functions for hot paths (removes stack frames, faster than `functools.partial`).

### Hybrid Routing
Use `matchit` (Rust) for performance with standard ASGI fallback for safe, incremental adoption.

### Bun FFI String Handling
Passing strings to C ABI via Bun `dlopen` requires:
- Manual null-termination: `Buffer.from(s + '\0')` for `ptr` type
- Or use `cstring` type (may be flaky)

## Process Supervision

### Dual Reaping Race
When mixing `libc::waitpid` (Rust) and `subprocess.Popen.poll()` (Python):
- Both compete to reap child processes
- **Solution**: Python actively handles exits from `poll()` as fallback for Rust supervisor

### Windows Support
Job Object support is enabled but needs implementation to ensure child processes are killed when parent dies.

## Build & Distribution

### PyO3 GIL Release
Use `py.detach` (not deprecated `py.allow_threads`).

### Bun.build Output
Native builds (`target: "bun"`) ignore `outfile` option; require post-processing move for specific output locations.

### CLI Composition
Split large `click` CLI into modules (`serve.py`, `build.py`, `init.py`) and aggregate in `__init__.py`.

---

*See `.agent/archive/historical-decisions.md` for full historical context from archived PRDs.*
