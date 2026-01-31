# Vertebra Architecture

## Overview

Vertebra is a Python-first application backbone with a high-performance Rust Broker and IPC primitives. The core goals are:

- Zero-copy IPC for high-throughput workloads.
- Resilient multi-process architecture (Broker + Workers).
- Cross-platform parity (Linux/macOS/Windows) for the MVP.

## Active Specifications & Roadmap

**Vertebra 1.0 Roadmap**

| Spec | Focus | Status |
| --- | --- | --- |
| **Quality** | Test fixes, API renaming (`Vertebra*`), Type auditing | Active (P0) |
| **Database** | SQLSpec/SQLAlchemy/AA integration, Serialization | Active |
| **Server** | MCP Protocol, Pool Health, Observability | Active |
| **Distribution** | `vertebra init`, `vertebra build`, Rust Supervisor | Active |
| **Docs** | Integration tests, Benchmarks, Examples | Active |

## Component Map

```
Python (Litestar-style API)
  ├── Type Bridge + Tooling
  │    └── Shared metadata registry
  ├── ORM Extensions
  │    ├── SQLSpec (Vertebra* adapters)
  │    ├── SQLAlchemy (Native Dialects)
  │    └── Advanced Alchemy
  ├── Resources
  │    └── AsyncBrokerClient → shared memory → Rust broker
  ├── MCP
  │    └── @mcp_tool decorator, schema generation
  └── Server
       └── ASGI worker with IPC framing

Rust (IPC Core + Broker)
  ├── vertebra-core
  │    ├── Shared memory (ShmRegion)
  │    ├── Ring Buffers (SPSC/MPMC)
  │    └── Sync primitives (futex/ulock)
  ├── vertebra-py
  │    ├── Python bindings (PyO3)
  │    └── Supervisor (Process lifecycle, GIL-free)
  └── vertebra-server (Broker)
       ├── HTTP/WebSocket server (Hyper/Tokio)
       ├── Connection Pools
       ├── Dispatcher (LeastPending)
       └── Observability
```

## Cross-PRD Decisions (Canonical)

- **Python version**: 3.12+.
- **Serialization**:
  - Python codec: msgspec
  - MCP/gRPC control plane: Protobuf
  - Driver data plane: MessagePack (small) + Arrow IPC (large)
  - Rust internal IPC: bincode
  - Large structured payloads: Arrow (data-plane only)
- **IPC transport**:
  - Driver/broker data plane: shared memory ring buffers (no sockets)
  - Control plane: in-process or shared memory (no TCP)
- **Runtime strategy**: Rust-first Broker (vertebra-server).
- **Test infrastructure**: Docker/Podman runtime abstraction for integration tests (no compose required).

## Testing Layout (Python)

Python tests are organized by **unit** vs **integration** to make test cost explicit:

```
src/py/tests/
  conftest.py
  database.py
  helpers/
  unit/
  integration/
```

- **Unit**: fast, in-memory tests (no external services).
- **Integration**: tests that rely on shared memory IPC, broker processes, or containerized services.

Commands:
- Unit-only (default): `uv run pytest src/py/tests`
- Integration: `uv run pytest src/py/tests -m integration --integration`
- All tests (Rust + Python unit + integration): `make test-all`

## Shared-Memory Broker (vertebra-server)

The broker is the central resource manager, owning all database and storage pools, and dispatching requests to Python workers.

```
┌─────────────────────────────────────────────────────────────────┐
│                    Rust Broker Process (Hyper)                  │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────┐ │
│  │ SqlPool  │  │SqlitePool│  │RedisPool │  │  StoragePool     │ │
│  │ (PG/MySQL)│ │          │  │          │  │ (Local/S3/GCS)   │ │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────────┬─────────┘ │
│       └──────────┬──┴───────────┬┴─────────────────┬┘           │
│                  │              │                  │            │
│              BasePoolAdapter (counters, stats)                  │
├─────────────────────────────────────────────────────────────────┤
│             LeastPendingDispatcher (Load Balancing)             │
├─────────────────────────────────────────────────────────────────┤
│             Shared Memory Ring Buffers (SPSC/MPMC)              │
└────────────────────────────┬────────────────────────────────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
        Python Worker   Python Worker   Python Worker
```

### Dispatcher Architecture

The `Dispatcher` trait allows different load balancing strategies. The default `LeastPendingDispatcher`:
- Tracks active request counts for each worker.
- Routes new requests to the worker with the lowest pending count.
- Uses lock-free atomic counters for low overhead.

### ChannelHandle (Bidirectional IPC)

`ChannelHandle` provides paired SPSC rings for request/response patterns between the Rust Broker and Python Workers.

## Performance Strategy

- Use shared memory for zero-copy transfer where possible.
- Favor small, structured IPC messages; reserve Arrow for large, columnar data.
- Maintain backpressure and bounded queues to preserve tail latency.
- **Optimistic GIL**: Python workers attempt non-blocking IPC operations first, only releasing the GIL if they must wait.

## Cross-Platform Notes

All public APIs must avoid Linux-only assumptions. Platform-specific code lives
behind trait/abstraction boundaries so macOS and Windows parity remains viable.

## Repository Structure

```
src/
  py/        # Python package (vertebra)
  rs/        # Rust workspace (core, py-bindings, server)
tools/
  lib/       # Tooling helpers (container runtime abstraction)
```