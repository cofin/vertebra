# PRD: Shared Memory Broker (Bun + C ABI)

## Goal

Provide **native-feel, zero-socket** SQL and storage access across Python and Bun by
using a central **Bun broker process** and a **shared-memory C ABI transport**. All
requests flow through shared memory ring buffers. Large result sets use Arrow IPC
for zero-copy reads. Small results use MessagePack via msgspec.

## Non-Negotiables

- **No sockets/TCP** in the data plane (no UDS, no TCP, no pipes).
- **Central broker authority**: the Bun broker owns connection pools.
- **Native feel** for Python: CFFI calls into a C ABI library.
- **Bun SQL compatibility** for Postgres/MySQL/SQLite.
- **Redis support** is required (native path inside the broker).
- **Provider registration** for non-Bun SQLSpec adapters.
- **Storage access** must also route through the shared-memory broker (fsspec/object store).

**Decision Note (2026-01-19)**: Directly embedding Bun as a shared library for Python is not feasible today
(no stable libbun ABI). The canonical architecture is a **Bun broker sidecar** accessed via
shared memory + C ABI. The broker protocol must remain extensible so a future **Rust-native
driver layer** can be loaded in-process by both Bun and Python without changing client configs.

## Architecture Overview

```
Python (SQLSpec)        Bun Broker (Bun SQL)
  |  CFFI + C ABI  <-->  |  bun:ffi + C ABI
  |  shm ring buffers     |  shm ring buffers
  |  msgspec + Arrow       |  msgspec + Arrow

Other providers (Python SQLSpec adapters)
  |  CFFI + shm protocol
  |  register with broker
```

### Components

1. **C ABI Shared Memory Library (Rust)**
   - Creates/opens shared memory regions.
   - Exposes ring buffer send/recv, waits, and stats.
   - Provides Arrow IPC helpers for large result sets.

2. **Bun Broker**
   - Owns pools for Postgres/MySQL/SQLite (Bun SQL).
   - Owns Redis pools (native implementation inside broker).
   - Owns storage adapters (fsspec-compatible backends / object store).
   - Fairness scheduler across clients.
   - Routes requests to registered providers (SQL + storage).

3. **Python Client + SQLSpec Integration**
   - CFFI bindings for shared memory transport.
   - SQLSpec adapter that targets the broker.
   - Generic plugin registration: `Vertebra(plugins=[...])` / `register_plugin(...)`.

4. **Provider Host**
   - Python process that registers custom SQLSpec adapters or storage backends.
   - Uses the same shared-memory protocol to receive/handle broker requests.
   - Registration includes provider ring names for direct broker routing.

## Data Plane Encoding

- **Control + Small results**: MessagePack via msgspec (Python) and msgpack (Bun).
- **Large results**: Arrow IPC stream in shared memory.
- **Arrow threshold (initial)**: payload >= 1MB **or** rows >= 1,000.

## Key Requirements

- **Shared memory only** for requests/responses.
- **Zero-copy** where possible (Arrow, msgspec buffer protocol).
- **No sockets** for driver/broker communication.
- **Broker is single source of truth** for pools.
- **Multi-process** clients must connect via shared memory and semaphores.

## Out of Scope

- SQLAlchemy dialect (deferred until last).
- Advanced Alchemy plugin (deferred until last).
- In-process Bun embedding (requires libbun ABI; deferred).

## Success Criteria

- Python can execute SQLSpec queries via the broker with no sockets.
- Storage operations can route through the broker with the same C ABI transport.
- Bun broker handles all pools and fairness scheduling.
- Arrow IPC used for large result sets with zero-copy reads.
- Non-Bun SQLSpec adapters can register and execute through broker.
