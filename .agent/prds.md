# PRD Registry

This file tracks all PRDs (Product Requirements Documents) for the project. Each PRD has its own detailed plan in its respective folder.

---

## Active PRDs

### [ ] Rust-Based Arrow Encoder (Optimization)
*Link: [./specs/rust-arrow-encoder/](./specs/rust-arrow-encoder/)*
*Risk Level: medium*

Implement a high-performance Arrow IPC encoder in Rust (vertebra-py) to replace Python row-to-column pivot logic, targeting >5x speedup for 1000+ rows.

### [~] Security & User Framework
*Link: [./specs/security-user-framework/](./specs/security-user-framework/)*
*Risk Level: medium*
*Status: Phase 2 complete, Phase 3 in progress*

Comprehensive security and user management framework with broker-first authentication.

### [ ] Documentation & Adoption
*Link: [./specs/documentation-adoption/](./specs/documentation-adoption/)*
*Risk Level: low*
*Duration: 18 weeks*

Integration tests, benchmarks, examples, migration guides, and documentation for 1.0.

### [ ] Fix Startup Module Import and Polish CLI
*Link: [./fix-startup-module-import-and-polish-cli/](./fix-startup-module-import-and-polish-cli/)*

### [ ] Native App Framework
*Link: [./specs/native-app-framework/](./specs/native-app-framework/)*
*Risk Level: high*

Native Vertebra App framework with route decorators and real-time state synchronization via opcode-based field deltas over SPSC rings. Litestar-aligned patterns (Router, Controller, Guards) with universal DI and extreme performance targets.

---

## Specs in .agent/specs/

| Spec | Status | Description |
|------|--------|-------------|
| `asgi-server-benchmarks` | Active | ASGI server performance comparisons |
| `cli-broker-status-ipc` | Active | CLI status command IPC |
| `cli-polish` | Active | CLI UX improvements |
| `ipc-performance-optimization` | Active | IPC latency optimizations |
| `ipc-startup-fix` | Active | Startup race conditions |
| `native-app-framework` | Planned | Native app with opcode-based state sync |
| `native-multipart-fallback` | Active | Multipart request handling |
| `optional-dependency-handling` | Active | Optional dependency patterns |
| `packaging-distribution` | Active | Package distribution |
| `pluggable-runtime-layer` | Active | Runtime abstraction |

---

## Archived PRDs

See `specs/archive/README.md` for historical reference.

**Completed:**
- PRD-0: Foundation
- PRD-1: Rust IPC
- PRD-2: Python Bindings
- PRD-3: Bun Server
- PRD-5: Resource Management (partial)
- eventfd-async-transport
- test-deadlock-fix

**Superseded:**
- PRD-4: Bun Drivers (replaced by shared-memory broker)
- driver-benchmark (architecture changed)

**Merged:**
- bun-broker-pool-manager → production-readiness
- sqlalchemy-integration → orm-extensions
