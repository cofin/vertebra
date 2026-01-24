# Research: Production Readiness

**Date:** 2026-01-18
**Focus:** Production Readiness Gaps & Integration Strategy

## 1. Scope Analysis

The "Production Readiness" scope encompasses five distinct pillars:
1.  **Server Foundation**: Closing the feature gap with standard ASGI servers (Streaming, WebSockets).
2.  **Resource Management**: Moving from local pools to the `vertebra-broker` architecture.
3.  **Driver Ecosystem**: Standardizing driver access via `sqlspec` and `fsspec`.
4.  **MCP Integration**: Moving from a skeleton to a functional agent interface.
5.  **Quality & Polish**: Eliminating technical debt (strict typing, anti-patterns).

## 2. Technical Feasibility

### 2.1 Server Streaming & WebSockets
- **Bun API**: Bun's `File` and `Response` objects support streaming via `ReadableStream`.
- **ASGI Bridge**: Implemented mapping of `ReadableStream` chunks to `http.request` IPC frames (2026-01-19).
- **Challenge**: Backpressure. We need to pause the Bun stream if the IPC channel is full.
- **Solution**: Use `await` on IPC send; backpressure now propagates to Bun.

### 2.2 Resource Broker
- **Current State**: `vertebra-broker` crate exists but is a standalone binary.
- **Integration**: Needs to be lazily spawned by the Python runtime (or Bun runtime) if not present.
- **IPC**: Shared-memory C ABI (no sockets/UDS/TCP) for all broker access.
- **Decision**: For Phase 2, we might stick to *local* fair pooling within the worker process (Python-side) or Bun-side, before fully committing to the external broker process, *unless* the broker is essential for cross-process coordination (which it is for a multi-worker setup).
- **Consensus**: We will proceed with the `vertebra-broker` integration as the "correct" architecture.

### 2.3 SQLSpec & fsspec
- **SQLSpec**: Require implementing `AsyncDriver`, `AsyncConnection`, `AsyncCursor`. The existing `vertebra.drivers` classes are close but need an adapter wrapper.
- **fsspec**: Requires `AbstractFileSystem` implementation. `vertebra.storage` classes need to map `ls`, `open`, `info`, etc. to the fsspec API.

### 2.4 Code Quality
- **Anti-Patterns**: `from __future__ import annotations` removal is straightforward but touches 50+ files.
- **Typing**: 120 errors is manageable. `Optional` -> `| None` is a simple regex replace (mostly).

## 3. Implementation Strategy

We will use a **Phased Approach** within a single "Release Candidate" track.
Breaking this into 5 separate PRDs might fragment the "Release" goal.
We will maintain *one* PRD (`production-readiness`) but treat each Phase as a distinct checkpoint/milestone.

**Milestones:**
1.  **Server & CLI**: Usable `vertebra serve`.
2.  **Drivers & Storage**: Usable with standard tools.
3.  **Broker & Pools**: Robust under load.
4.  **MCP**: AI ready.
5.  **Polish**: Clean code.

## 4. Risks
- **Broker Complexity**: managing a separate process adds deployment complexity.
- **Mitigation**: Ensure `vertebra serve` manages the broker lifecycle transparently.
