# Analysis: Native Multipart vs ASGI Fallback

## Context
The Vertebra native pipeline is optimized for zero-copy, shared-memory ring buffers and low-latency JSON RPC traffic. Multipart/form-data file uploads introduce unbounded payload sizes, a streaming requirement, and bandwidth that can saturate IPC rings. The current architecture bypasses ASGI receive loops, so Litestar's multipart parsing is not available in the native path.

## Similar Implementations Found

1. `src/py/vertebra/extensions/litestar/native.py` - Litestar route reflection and native router insertion.
2. `src/ts/src/ipc/asgi.ts` - Bun-side ASGI bridge, routing, and direct.http dispatch.
3. `src/py/vertebra/server/worker.py` - Python ASGI worker loop and receive/send handling.
4. `specs/active/native-pipeline-litestar-compat/enhancements.md` - Existing multipart fallback rationale and notes.

## Patterns to Follow

- Route compatibility must be explicit; incompatible routes are excluded from native router.
- ASGI fallback is the correctness escape hatch for unsupported request/response types.
- Bun broker handles routing; Python worker expects ASGI scope + receive/send semantics.

## Key Constraints
- Shared-memory ring buffers are sized for small payloads (64KB to 1MB), not gigabyte uploads.
- Streaming is mandatory for file uploads; buffering in RAM or SHM is unsafe.
- Pushing large file chunks through the Bun broker would block control-plane traffic and health checks.
- Litestar expects an ASGI `receive` awaitable for multipart parsing; the native path does not provide one.

## Architectural Options
1. ASGI fallback (current): skip native routing for multipart routes and proxy to ASGI.
2. Native streaming via IPC: introduce `STREAM_CHUNK` opcodes and emulate ASGI receive in Python.
3. Side-channel transfer: write upload data directly to temp files or large SHM regions and pass handles to Python.
4. Rust streaming parser: strip multipart boundaries in Rust and expose clean file descriptors or memory maps.

## Evaluation Criteria
- Correctness and compatibility with Litestar expectations.
- Preservation of low-latency control traffic.
- Memory safety and backpressure behavior.
- Cross-platform feasibility (Linux/macOS/Windows).
- Engineering cost vs user-perceived latency impact.

## Initial Direction
Phase 1 should keep ASGI fallback for multipart routes. The performance benefit of native multipart parsing is minimal relative to network I/O, while the engineering complexity is high. Future phases can explore side-channel transfer and a Rust streaming parser when the IPC protocol and storage abstractions are ready.

## Deliverables for PRD Workflow
- Define acceptance criteria for safe fallback behavior.
- Document required IPC opcodes and parser components for future phases.
- Specify tests that confirm multipart routes are excluded from native routing.
- Clarify documentation updates for architecture and pipeline behavior.
