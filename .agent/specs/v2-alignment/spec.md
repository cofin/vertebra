# Specification: V2 Alignment

## Context
The project has shifted from a Hybrid (Python/Bun) architecture to a **Native Rust Core**. During setup, 7 active specifications were identified as STALE because they explicitly rely on the now-deleted Bun runtime (`src/ts`).

## Goal
Systematically review each stale specification and either:
1.  **Rewrite** it for the Rust architecture.
2.  **Archive** it if it's no longer relevant.
3.  **Merge** it into the V2 Master Plan.

## Scope
The following specs are targeted for review:
1.  `native-app-framework`
2.  `cli-broker-status-ipc`
3.  `native-multipart-fallback`
4.  `pluggable-runtime-layer`
5.  `asgi-server-benchmarks`
6.  `ipc-performance-optimization`
7.  `ipc-startup-fix`

## Success Criteria
- All stale specs are processed (Rewritten/Archived/Merged).
- `flows.md` has no "STALE" entries.
- Dead code (if any remains) is identified.
