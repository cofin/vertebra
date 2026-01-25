# Track Registry

This file tracks all tracks (features, bugs, refactors) for the project.
Each track has its own detailed spec and plan in its respective folder.

---

## Active Tracks

### [~] Security & User Framework
*Link: [./specs/security-user-framework/](./specs/security-user-framework/)*
*Status: Phase 2 complete, Phase 3 in progress*
*Risk Level: medium*

Comprehensive security and user management framework. Broker-first authentication with JWT/OAuth2/IAP validation. User model mixins for msgspec/Pydantic/SQLAlchemy.

**Progress:**
- [x] Phase 1: Bun IPC Security Hooks
- [x] Phase 2: Core User Framework
- [~] Phase 3: Broker-Centric Authentication (integration tests failing)
- [ ] Phase 4: Permissions & Scopes
- [ ] Phase 5: Vertebra Native Integration
- [ ] Phase 6: Quality & Documentation

---

### [ ] Documentation & Adoption
*Link: [./specs/documentation-adoption/](./specs/documentation-adoption/)*
*Status: Not started*
*Risk Level: low*
*Duration: 18 weeks*

Comprehensive documentation, integration tests, benchmarks, examples, and migration guides for Vertebra 1.0.

**Deliverables:**
- Integration test suite (90%+ coverage)
- Performance benchmarks vs Granian/Uvicorn
- 6 example applications
- Migration guides (litestar-vite, ASGI servers)
- API documentation (Python/TypeScript/Rust)
- Developer guides

---

### [ ] Fix Startup Module Import and Polish CLI
*Link: [./fix-startup-module-import-and-polish-cli/](./fix-startup-module-import-and-polish-cli/)*
*Status: Planned*

---

### [ ] Native App Framework
*Link: [./specs/native-app-framework/](./specs/native-app-framework/)*
*Status: Planned*
*Risk Level: high*

Native Vertebra App framework bypassing ASGI for extreme performance. Features Litestar-aligned patterns (Router, Controller, Guards, Events) with universal DI via clean `Inject[T]` syntax. Real-time state synchronization via opcode-based field deltas over SPSC rings with LWW resolution.

**Key Features:**
- Rust-based routing with `matchit` for O(1) matching
- AOT DI compilation eliminating runtime container lookups
- State sync: <100μs propagation, <100ns local reads
- SSE + WebSocket + Long-polling for frontend sync
- Universal DI across controllers, guards, middleware, events, CLI

---

## Migrated from .agent/specs/

The following specs were previously in `.agent/specs/` and remain active:

| Spec | Status | Description |
|------|--------|-------------|
| `asgi-server-benchmarks` | Active | ASGI server performance comparisons |
| `cli-broker-status-ipc` | Active | CLI status command IPC implementation |
| `cli-polish` | Active | CLI UX improvements |
| `ipc-performance-optimization` | Active | IPC latency and throughput optimizations |
| `ipc-startup-fix` | Active | Startup race conditions |
| `native-multipart-fallback` | Active | Multipart request handling |
| `optional-dependency-handling` | Active | Optional dependency patterns |
| `packaging-distribution` | Active | Package distribution strategy |
| `pluggable-runtime-layer` | Active | Runtime abstraction layer |

---

## Archived Specs

See `specs/archive/README.md` for completed/superseded specs:
- PRD-0 through PRD-4 (Foundation, IPC, Bindings, Server, Drivers)
- eventfd-async-transport (Completed)
- test-deadlock-fix (Completed)
- bun-broker-pool-manager (Merged into production-readiness)
- driver-benchmark (Superseded by architecture changes)
- sqlalchemy-integration (Merged into orm-extensions)
