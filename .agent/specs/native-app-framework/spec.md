# Native Vertebra App Framework

## Overview

A high-performance native application framework for Vertebra that bypasses ASGI overhead for its own routes, while retaining full compatibility for serving standard ASGI applications. It provides real-time state synchronization via opcode-based field deltas over SPSC rings. The framework follows Litestar-aligned patterns (Router, Route, Controller, Guards, Events) while leveraging Rust and Bun for extreme performance optimization.

**Key Goals:**
- Sub-100μs IPC latency for state updates
- >200K req/s throughput (simple)
- Real-time state sync across Python workers
- Universal dependency injection across all layers
- Clean, type-safe API with `Inject[T]` pattern
- **Dual-Mode:** Run high-speed Native routes alongside standard ASGI apps

## Research Reference

- **Control Plane:** Custom Zero-Copy Structs (16-byte aligned headers) + MsgPack payloads.
- **Serialization:** `msgspec` (Python) and `rmp-serde` (Rust) are mandated.
- **Large Payloads:** Rust-based Arrow encoding is required to meet performance targets.

## Functional Requirements

### Must Have

#### Native App & Routing
- [ ] `Vertebra` class as the single unified application entry point and configuration hub (replaces `VertebraApp`/`App`).
- [ ] **Dual Routing:**
    - [ ] Native Routes: Resolved by Rust `matchit` -> Native Controller (Bypasses ASGI)
    - [ ] ASGI Mounts: Resolved by Rust `matchit` -> ASGI Adapter (Legacy/Compat)
- [ ] `Router` for grouping routes with shared middleware/guards
- [ ] `Route` decorators (`@get`, `@post`, `@put`, `@delete`, `@patch`, `@websocket`)
- [ ] `Controller` base class for grouping related routes
- [ ] Path parameters with type coercion (`/users/{user_id:int}`)

#### State Synchronization
- [ ] `NativeState` with opcode-based field deltas
- [ ] **Protocol:** Custom Zero-Copy Header (16-byte) + MsgPack Payload
- [ ] SPSC ring buffers for state updates between workers
- [ ] Bun Broker as central state hub with LWW (Last-Writer-Wins) resolution
- [ ] Local shadow state in Python workers for nanosecond reads

#### Dependency Injection
- [ ] `Inject[T]` type annotation for handler parameters (clean consumption)
- [ ] Simplified provider registration (avoid verbose `@provide` decorators)
- [ ] AOT compilation of dependency graphs at startup
- [ ] Universal DI across: Controllers, Guards, Middleware, Events, CLI
- [ ] `VertebraContext` abstraction for all entry points

#### Guards & Middleware
- [ ] Guard system with `requires()` decorator pattern
- [ ] Native guards in Rust for performance (JWT validation, rate limiting)
- [ ] Middleware chain with before/after hooks
- [ ] Guard result caching within request scope

#### Events & Lifecycle
- [ ] Application lifecycle hooks (`on_startup`, `on_shutdown`)
- [ ] Event system for pub/sub within the application
- [ ] Background task support with DI access

#### ASGI Compatibility & Litestar Integration
- [ ] **Litestar CLI Extension:** `VertebraPlugin` that overrides the standard `litestar run` command.
    - [ ] Users add `VertebraPlugin()` to their `Litestar` app.
    - [ ] `litestar run` automatically launches the Vertebra Supervisor/Runtime instead of Uvicorn.
    - [ ] Optimizes the Litestar app startup by inspecting routes and mounting them natively where possible.
- [ ] `--asgi` flag / mode to run generic ASGI apps (FastAPI, Django).

### Should Have

#### Frontend Sync
- [ ] SSE (Server-Sent Events) for state broadcasting
- [ ] WebSocket support for bidirectional communication
- [ ] Long-polling fallback for restricted environments

#### Response Handling
- [ ] Automatic JSON serialization via `msgspec` (Strict Requirement)
- [ ] Streaming response support (NDJSON, SSE)
- [ ] File responses with range request support

### Nice to Have

- [ ] OpenAPI schema generation
- [ ] GraphQL integration layer
- [ ] gRPC support via shared-memory transport

## Non-Functional Requirements

### Performance
| Metric | Target | Notes |
|--------|--------|-------|
| IPC latency | <100μs | State sync round-trip |
| State read (local shadow) | <100ns | From worker shadow state |
| Throughput (simple JSON) | >200K req/s | Native implementation |
| Throughput (with state sync) | >150K req/s | Including delta propagation |
| Memory allocation rate | <1MB/s | Under sustained load |
| Route matching | O(1) | Rust matchit |
| DI resolution (compiled) | ~0 overhead | AOT removes container lookups |

### Security
- JWT validation in Rust (no Python GIL)
- Guard execution before handler invocation
- Scoped dependency lifetimes (request, session, app)

## Technical Approach

### Architecture Split

```
┌─────────────────────────────────────────────────────────────┐
│                    Native App Architecture                   │
├─────────────────────────────────────────────────────────────┤
│  RUST CORE         │  BUN IO LAYER      │  PYTHON LOGIC     │
│  ───────────────   │  ──────────────    │  ─────────────    │
│  • Router(matchit) │  • HTTP/WS server  │  • Vertebra (Config)│
│  • Custom Header   │  • SSE broadcast   │  • Controllers    │
│  • Native Guards   │  • Long-polling    │  • Domain logic   │
│  • Dep Graph Cache │  • Static assets   │  • Inject[T]      │
│  • Ring Buffers    │  • Connection mgmt │  • Events         │
│                    │                    │  • ASGI Adapter   │
└─────────────────────────────────────────────────────────────┘
```

### Protocol: Zero-Copy Header + MsgPack

**Header (16 bytes, aligned):**
- `id` (u64): Correlation ID
- `flags` (u32): Status, Compression, Priority
- `len` (u32): Payload length

**Payload:**
- Small: MessagePack (`msgspec`)
- Large: Arrow IPC (Zero-Copy Shm Ref)

### Zero-Copy Boundaries

This architecture implements a pragmatic approach to zero-copy, optimizing where it matters most:

1.  **Control Plane (Headers):**
    *   **Condition:** Always.
    *   **Mechanism:** Fixed 16-byte struct (`RequestHeader`) read directly from shared memory via pointer cast (Rust) or `DataView` (TS). No allocation or parsing occurs for routing decisions.

2.  **Data Plane (Large Payloads > 1k rows):**
    *   **Condition:** When `Arrow` encoding is triggered (by row count or byte size threshold).
    *   **Mechanism:**
        *   **Producer:** Writes directly to a dedicated Shared Memory Region.
        *   **Consumer:** Maps the region and creates an Arrow `RecordBatch`/`Table` view over the raw bytes. **Zero data copies** to heap.
        *   **Network:** Future Arrow Flight integration will allow `sendfile`/`splice` from these regions.

3.  **Data Plane (Small Payloads):**
    *   **Condition:** Standard JSON/MessagePack responses.
    *   **Mechanism:** **NOT Zero-Copy.**
        *   Data is serialized to buffer -> Written to Ring -> Read from Ring -> Deserialized.
        *   *Rationale:* For small payloads (<1KB), the overhead of object allocation and serialization logic dominates `memcpy` cost. The complexity of zero-copy serialization for dynamic small objects outweighs the nanosecond-scale gains.

4.  **IPC Transport (Ring Buffers):**
    *   **Mechanism:** SPSC Rings in Shared Memory.
    *   **Zero-Copy Aspect:** No kernel context switches or syscalls (`read`/`write`) for data transfer. It is purely userspace memory access.

### State Sync: Opcode-Delta Ring

```
Python Worker A                    Bun Broker                    Python Worker B
     │                                 │                              │
     │  ┌─────────────────────┐        │                              │
     │  │ user.name = "Alice" │        │                              │
     │  └─────────────────────┘        │                              │
     │            │                    │                              │
     │   Header + Delta Payload        │                              │
     │  ──────────────────────────────>│                              │
     │                                 │  LWW Resolution              │
     │                                 │  Update Authoritative        │
     │                                 │  ─────────────────────────>  │
     │                                 │   Header + Delta Payload     │
     │                                 │                              │
     │                                 │  <─────────────────────────  │
     │                                 │   ACK (Header only)          │
     │  <──────────────────────────────│                              │
     │   ACK (Header only)             │                              │
```

**Opcodes (Payload):**
| Opcode | Name | Payload |
|--------|------|---------|
| 0x01 | FIELD_SET | (field_id, value, timestamp) |
| 0x02 | FIELD_DELETE | (field_id, timestamp) |
| 0x03 | OBJECT_REPLACE | (field_id, new_object, timestamp) |
| 0x04 | SYNC_REQUEST | (state_id) |
| 0x05 | SYNC_RESPONSE | (full_state) |

### Files to Modify/Create

**New Files:**
- `src/py/vertebra/vertex.py` - Vertex class (Unified Config Entry Point)
- `src/py/vertebra/routing/` - Router, Route, decorators
- `src/py/vertebra/controllers/` - Controller base class
- `src/py/vertebra/di/` - Inject, Provider, AOT compiler
- `src/py/vertebra/guards/` - Guard system
- `src/py/vertebra/events/` - Event system
- `src/py/vertebra/middleware/` - Middleware chain
- `src/rs/vertebra-core/src/router.rs` - Rust matchit wrapper
- `src/rs/vertebra-core/src/state_manager.rs` - LWW state hub
- `src/rs/vertebra-core/src/guards.rs` - Native guards
- `src/ts/src/broker/state-hub.ts` - Bun state synchronization

**Modified Files:**
- `src/py/vertebra/state.py` - Extend NativeState with opcode deltas
- `src/rs/vertebra-core/src/ring.rs` - Add state delta opcodes
- `src/ts/src/broker/serve.ts` - Add state hub service
- `src/py/vertebra/asgi.py` - Update adapter to use new IPC protocol
- `src/py/vertebra/extensions/litestar/cli.py` - Implement `VertebraPlugin` CLI override

## Acceptance Criteria

- [ ] `Vertex` class handles both Native Routes and ASGI Mounts
- [ ] `VertebraPlugin` correctly overrides `litestar run` to launch Vertebra Runtime
- [ ] Routes resolve in O(1) time via Rust matchit
- [ ] State changes propagate to all workers in <100μs using Custom Header protocol
- [ ] `Inject[T]` works in controllers, guards, middleware, events
- [ ] Guards execute before handlers with DI access
- [ ] SSE/WebSocket/long-polling deliver state updates to frontend
- [ ] Simple JSON throughput >200K req/s
- [ ] All tests pass with >90% coverage

## Out of Scope

- GraphQL/gRPC
- Database migrations
- Admin UI

## Dependencies

**Internal:**
- `vertebra-core` Rust crate
- `vertebra-py` PyO3 bindings
- `vertebra-js` napi-rs bindings

**External:**
- `matchit` (Rust)
- `msgspec` (Python)
- `dishka` patterns (inspiration only)
