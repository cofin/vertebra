# Vertebra Architecture

## Overview

Vertebra is a Python-first application backbone with Rust IPC primitives and a Bun
(TypeScript) server/runtime layer. The core goals are:

- Zero-copy IPC for high-throughput workloads.
- tRPC-like developer experience with Python → TypeScript type bridging.
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
  │    └── Shared metadata registry / IR / TypeScript generator
  ├── ORM Extensions
  │    ├── SQLSpec (Vertebra* adapters)
  │    ├── SQLAlchemy (Native Dialects)
  │    └── Advanced Alchemy
  ├── Resources
  │    └── BrokerClient → shared memory → Bun broker
  ├── MCP
  │    └── @mcp_tool decorator, schema generation
  └── Server
       └── ASGI worker with IPC framing

Rust (IPC Core + Supervisor)
  ├── vertebra-core
  │    ├── Shared memory (ShmRegion)
  │    ├── Ring Buffers (SPSC/MPMC)
  │    └── Sync primitives (futex/ulock)
  ├── vertebra-py
  │    ├── Python bindings
  │    └── **Supervisor** (Process lifecycle, GIL-free)
  └── vertebra-js
       └── Napi bindings for Bun

TypeScript/Bun (Server + Broker)
  ├── Broker
  │    ├── Connection Pools (SQL, Redis, Storage)
  │    └── FairnessScheduler
  ├── Server
  │    ├── HTTP/WebSocket server
  │    ├── ASGI bridge
  │    └── **Observability** (/__metrics__)
  └── MCP Server
       ├── Transports (stdio, SSE, WebSocket)
       └── IPC Bridge to Python
```

## Cross-PRD Decisions (Canonical)

- **Python version**: 3.12+.
- **Directory layout**: `src/ts/` for TypeScript/Bun code.
- **Serialization**:
  - Python codec: msgspec
  - MCP/gRPC control plane: Protobuf
  - Driver data plane: MessagePack (small) + Arrow IPC (large)
  - Rust internal IPC: bincode
  - Large structured payloads: Arrow (data-plane only)
- **IPC transport**:
  - Driver/broker data plane: shared memory ring buffers (no sockets)
  - Control plane: in-process or shared memory (no TCP)
- **Runtime strategy**: Bun-first; Node fallback only if parity is proven.
- **Test infrastructure**: Docker/Podman runtime abstraction for integration tests (no compose required).

## Shared-Memory Broker

The broker is the central resource manager, owning all database and storage pools:

```
┌─────────────────────────────────────────────────────────────────┐
│                         Bun Broker Process                      │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────┐ │
│  │ SqlPool  │  │SqlitePool│  │RedisPool │  │  StoragePool     │ │
│  │ (PG/MySQL)│ │          │  │          │  │ (Local/S3/GCS)   │ │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────────┬─────────┘ │
│       └──────────┬──┴───────────┬┴─────────────────┬┘           │
│                  │              │                  │            │
│              BasePoolAdapter (counters, stats)                  │
├─────────────────────────────────────────────────────────────────┤
│                    FairnessScheduler (FIFO/Weighted/Priority)   │
├─────────────────────────────────────────────────────────────────┤
│                    Shared Memory Ring Buffers (SPSC)            │
└────────────────────────────┬────────────────────────────────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
        Python Worker   Python Worker   Python Worker
```

### Pool Architecture

All pools extend `BasePoolAdapter` which provides:
- Counter tracking (acquires, releases, errors)
- Unified `PoolStats` type for metrics
- Common lifecycle (acquire/release/getStats)
- Health checks (periodic validation)
- Lifecycle hooks (onAcquire, onRelease, onError)

### ChannelHandle (Bidirectional IPC)

`ChannelHandle` provides paired SPSC rings for request/response patterns:

```typescript
// TypeScript usage
const { client, server } = createChannelPair(64, 8);
client.send(Buffer.from([1, 2, 3]), 0);
const response = server.recv(1024);

// Named channels for cross-process IPC (Unix)
const channel = ChannelHandle.createNamed("/my-channel", 64, 8, ChannelRole.Client);
channel.unlink(); // Explicit cleanup
```

Methods: `send()`, `trySend()`, `sendTimeout()`, `recv()`, `tryRecv()`, `recvTimeout()`, `unlink()`

## Performance Strategy

- Use shared memory for zero-copy transfer where possible.
- Favor small, structured IPC messages; reserve Arrow for large, columnar data.
- Maintain backpressure and bounded queues to preserve tail latency.

## Cross-Platform Notes

All public APIs must avoid Linux-only assumptions. Platform-specific code lives
behind trait/abstraction boundaries so macOS and Windows parity remains viable.

## Repository Structure

```
src/
  py/        # Python package (vertebra)
  rs/        # Rust workspace
  ts/        # Bun/TypeScript workspace
tools/
  lib/       # Tooling helpers (container runtime abstraction)
```