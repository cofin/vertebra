# Native App Framework

> Native Vertebra App framework with route decorators and real-time state synchronization via opcode-based field deltas over SPSC rings.

## Quick Links

- [Specification](./spec.md) - Full requirements and technical approach
- [Implementation Plan](./plan.md) - Phased task breakdown

## Status

| Phase | Status | Description |
|-------|--------|-------------|
| 1 | [ ] | Core Routing Infrastructure |
| 2 | [ ] | Dependency Injection System |
| 3 | [ ] | Guards and Middleware |
| 4 | [ ] | State Synchronization |
| 5 | [ ] | Frontend Sync Protocols |
| 6 | [ ] | Application Lifecycle |
| 7 | [ ] | Integration and Performance |
| 8 | [ ] | Documentation and Examples |

## Key Features

- **Native Routing**: Rust-based `matchit` router with O(1) matching
- **Clean DI**: `Inject[T]` pattern across all layers
- **Real-time State**: Opcode deltas over SPSC rings with LWW
- **Litestar-aligned**: Router, Controller, Guard patterns
- **Universal DI**: Works in controllers, guards, middleware, events, CLI

## Performance Targets

| Metric | Target |
|--------|--------|
| IPC latency | <100μs |
| State read | <100ns |
| Throughput (simple) | >200K req/s |
| Throughput (with sync) | >150K req/s |
| Memory alloc | <1MB/s |

*Baseline: Granian ASGI ~70-100K req/s. Native targets 2-3x improvement.*

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Native App Architecture                   │
├─────────────────────────────────────────────────────────────┤
│  RUST CORE         │  BUN IO LAYER      │  PYTHON LOGIC     │
│  ───────────────   │  ──────────────    │  ─────────────    │
│  • Router(matchit) │  • HTTP/WS server  │  • Controllers    │
│  • State LWW CRDT  │  • SSE broadcast   │  • Services       │
│  • Native Guards   │  • Long-polling    │  • Domain logic   │
│  • Dep Graph Cache │  • Static assets   │  • Inject[T]      │
│  • Ring Buffers    │  • Connection mgmt │  • Events         │
└─────────────────────────────────────────────────────────────┘
```

## Risk Level

**High** - Core architectural change requiring:
- Rust router integration
- State synchronization protocol
- AOT DI compilation
- Multi-worker coordination
