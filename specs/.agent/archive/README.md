# Archived PRDs

These Product Requirements Documents have been archived because the work is complete or the content has been consolidated.

## Archived PRDs

### PRD-0-foundation.md (Complete)
Foundation and repository setup. Build system configured, workspaces created, documentation complete.

### PRD-1-rust-ipc.md + PRD-1-rust-ipc/ (Complete)
Rust IPC core layer. Shared memory, SPSC/MPMC channels, buffer pools, sync primitives all implemented.

### PRD-2-python-bindings.md + PRD-2-python-bindings/ (Complete)
Python bindings and type bridge. @procedure decorator, type IR, extractor, serializer, TypeScript generator done.

### PRD-3-bun-server.md (Complete)
Bun server and protocol support. HTTP/1.1, HTTP/2, WebSocket, ASGI bridge, worker manager all done.

### PRD-4-bun-drivers.md + PRD-4-bun-drivers/ (Superseded)
Original socket-based driver architecture. Replaced by shared-memory broker. SQLSpec integration handles database access now.

---

## Active PRDs

The following PRDs remain active in `docs/prd/`:

- **PRD-5-resource-management.md** - Pool fairness, health checks (partially done)
- **PRD-6-integration.md** - Testing, documentation, examples (not started)
- **PRD-7-mcp-integration.md** - MCP protocol (in progress)
- **PRD-8-production-readiness.md** - Meta-tracking document

## Consolidated Requirements

See `/specs/active/consolidated/prd-remaining-work.md` for the unified remaining work document.

These specifications have been archived because they are either:
- **Completed**: The work has been fully implemented
- **Superseded**: The architecture has changed and the spec no longer applies
- **Merged**: The content has been consolidated into another spec

## Archived Specs

### driver-benchmark/ (Superseded)
**Reason**: Misaligned with architecture. Vertebra doesn't expose a direct `vertebra.connect()` API - it uses broker-backed adapters via SQLSpec. The "Driver Bet" hypothesis comparing raw driver speeds is invalid; Vertebra's value is shared pooling, not raw driver performance.

### bun-broker-pool-manager/ (Merged)
**Reason**: Already implemented differently. The `BasePoolAdapter` pattern is in `src/ts/src/broker/base-pool.ts`. Remaining work (health checks, idle tracking) is in `production-readiness/`.

### eventfd-async-transport/ (Completed)
**Reason**: Fully implemented in commit f354dbd (2026-01-19). The async ring buffer with eventfd notification is working. Phases 1-3 complete; Phase 4 migration is operational cleanup.

### sqlalchemy-integration/ (Merged)
**Reason**: Duplicates content in `orm-extensions/`. The orm-extensions spec is more comprehensive and covers SQLAlchemy alongside Advanced Alchemy and serialization.

---

## When to Archive vs Delete

- **Archive**: If the spec has historical value or documents architectural decisions
- **Delete**: If the spec was a draft that never became relevant

All archived specs are kept for reference but should not be used for new work.
