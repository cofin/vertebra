# Vertebra Architecture

## Overview

Vertebra is a Python-first application backbone with Rust IPC primitives and a Bun
(TypeScript) server/runtime layer. The core goals are:

- Zero-copy IPC for high-throughput workloads.
- tRPC-like developer experience with Python → TypeScript type bridging.
- Cross-platform parity (Linux/macOS/Windows) for the MVP.

## Project Status

**Overall:** PRD-1 in progress (Rust IPC core).

| PRD | Status | Notes |
| --- | --- | --- |
| PRD-0 | Complete | Foundation, build system, docs, repo cleanup |
| PRD-1 | Complete | Rust IPC core |
| PRD-2 | Not Started | Python bindings + type bridge |
| PRD-3 | Not Started | Bun server + protocol support |
| PRD-4 | Not Started | Bun-backed Python drivers |
| PRD-5 | Not Started | Resource management |
| PRD-6 | Not Started | Integration + docs |
| PRD-7 | Not Started | MCP integration |

## Component Map

```
Python (Litestar-style API)
  └── Type Bridge + Tooling (PRD-2)
       └── Shared metadata registry / IR

Rust (IPC Core)
  └── Shared memory + channels + sync primitives (PRD-1)
       └── PyO3 + napi-rs bindings

TypeScript/Bun (Server + Tooling)
  └── HTTP/2, H2C, WebSocket, JSON-RPC, gRPC (PRD-3)
       └── Client generation consumes PRD-2 registry
```

## Cross-PRD Decisions (Canonical)

- **Python version**: 3.12+.
- **Directory layout**: `src/ts/` for TypeScript/Bun code.
- **Serialization**:
  - Python codec: msgspec
  - MCP/gRPC control plane: Protobuf
  - Driver data plane: MessagePack
  - Rust internal IPC: bincode
  - Large structured payloads: Arrow (optional, data-plane only)
- **IPC transport**:
  - Unix: UDS + shared memory
  - Windows: named pipes + shared memory (TCP fallback)
- **Runtime strategy**: Bun-first; Node fallback only if parity is proven.

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
```

## Future Extensions

- Optional C ABI only if PRD-1 decision checklist triggers it.
- Additional transports (HTTP/3, WebSocket streaming) per PRD-3/PRD-7.
