# Vertebra Architecture

## Overview

Vertebra is a Python-first application backbone with Rust IPC primitives and a Bun
(TypeScript) server/runtime layer. The core goals are:

- Zero-copy IPC for high-throughput workloads.
- tRPC-like developer experience with Python → TypeScript type bridging.
- Cross-platform parity (Linux/macOS/Windows) for the MVP.

## Project Status

**Overall:** PRD-0/PRD-1/PRD-2 complete; PRD-3/PRD-4/PRD-5/PRD-8 in progress.

| PRD | Status | Notes |
| --- | --- | --- |
| PRD-0 | Complete | Foundation, build system, docs, repo cleanup |
| PRD-1 | Complete | Rust IPC core (SPSC/MPMC, shm, ChannelHandle) |
| PRD-2 | Complete | Python bindings + type bridge |
| PRD-3 | 90% | Bun server + protocol support (WebSocket, SSE pending) |
| PRD-4 | 75% | Bun-backed Python drivers (SQLSpec adapters complete) |
| PRD-5 | 60% | Resource management (broker pools, fairness scheduler) |
| PRD-6 | 5% | Integration + docs |
| PRD-7 | 10% | MCP integration (skeleton + IPC bridge stub) |
| PRD-8 | In Progress | Production readiness consolidation |

## Component Map

```
Python (Litestar-style API)
  ├── Type Bridge + Tooling (PRD-2)
  │    └── Shared metadata registry / IR / TypeScript generator
  ├── SQLSpec Extensions (PRD-4)
  │    └── Broker-backed adapters (Postgres/MySQL/SQLite)
  ├── Resources (PRD-5)
  │    └── BrokerClient → shared memory → Bun broker
  └── Server (PRD-3)
       └── ASGI worker with IPC framing

Rust (IPC Core)
  ├── vertebra-core
  │    ├── Shared memory (ShmRegion, shm_unlink_named)
  │    ├── SPSC/MPMC ring buffers with timeouts
  │    └── Sync primitives (futex/ulock/WaitOnAddress)
  ├── vertebra-js (napi-rs)
  │    ├── ChannelHandle (bidirectional SPSC pairs)
  │    ├── ChannelRole enum (Client/Server)
  │    └── BufferPool, SpscRing, MpmcRing handles
  └── vertebra-py (PyO3)
       └── Python bindings for channel/buffer/shm

TypeScript/Bun (Server + Tooling)
  ├── Broker (PRD-5)
  │    ├── Pool implementations (SqlPool, RedisPool, StoragePool)
  │    ├── BasePoolAdapter (shared counter tracking)
  │    └── FairnessScheduler
  ├── Server (PRD-3)
  │    ├── HTTP server + worker management
  │    ├── ASGI bridge with streaming
  │    └── Metrics + graceful shutdown
  └── MCP (PRD-7)
       └── JSON-RPC + IPC bridge stub
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

## Future Extensions

- Optional C ABI only if PRD-1 decision checklist triggers it.
- Additional transports (HTTP/3, WebSocket streaming) per PRD-3/PRD-7.
