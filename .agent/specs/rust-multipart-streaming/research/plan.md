# Research Plan and Background: Native Multipart Support vs ASGI Fallback

## 1. Research Scope and Purpose

This research document consolidates the technical context needed to produce a PRD for multipart/form-data handling in the Vertebra native pipeline. The goal is to explain why the current strategy is ASGI fallback and to define the requirements, constraints, and possible future paths toward native multipart support. The focus is on the interplay between the Bun broker, shared-memory ring buffers, the Python ASGI worker, and Litestar's expectations for multipart parsing. The research is grounded in the existing repository architecture and conventions, with direct reference to current code files and active specifications in `specs/active/`.

The provided problem statement asserts that multipart uploads are incompatible with the current native data plane due to unbounded payload size, the need for streaming, and the risk of saturating ring buffers. It also calls out that multipart parsing relies on the ASGI `receive()` loop that the native pipeline bypasses. This research validates those claims against the actual architecture and documents the costs, trade-offs, and potential alternative designs.

## 2. Current Architecture Overview

Vertebra is a Python-first application backbone with a shared-memory data plane. The architecture described in `docs/arch.md` defines a clear separation of responsibilities: Python provides the API surface, Bun hosts the broker and HTTP server, and Rust provides the shared-memory IPC primitives. The data plane avoids sockets entirely, using shared-memory ring buffers to minimize overhead and ensure predictable latency for request/response RPC flows. This matters because multipart uploads are not a conventional RPC pattern; they are streaming payloads whose size is potentially unbounded.

The canonical architecture has these major components:

- **Python (ASGI worker)**: Implements application logic, receives ASGI events, and sends ASGI responses back to Bun via IPC frames. The worker model is built around the ASGI receive/send pattern and assumes bounded request bodies for native handling.
- **Bun (HTTP server + broker)**: Accepts HTTP requests, attempts to route them through a fast native path using the shared-memory router, and falls back to ASGI for unsupported cases. Bun also owns pooled resources and enforces fairness in request scheduling.
- **Rust IPC core**: Provides ring buffers and synchronization primitives, exposing them to Python (PyO3) and Bun (`bun:ffi`). The ring buffers are SPSC and sized for fixed message slots.

This architecture is tuned for small, structured payloads, including JSON request bodies and MessagePack-encoded responses. The design explicitly calls out that large payloads should move through dedicated mechanisms (such as Arrow IPC for large query results). Multipart file upload streams do not fit cleanly into this paradigm.

## 3. Native Pipeline Request Flow

The Bun-side entry point to the native pipeline is the HTTP server and ASGI bridge. In `src/ts/src/ipc/asgi.ts`, the `AsgiBridge` accepts a `Request` and attempts to match it against a shared-memory router. If a match exists, the request is tagged with `type = "direct.http"` and a `handler_id` referencing a Python handler. Otherwise, the request is handled as a standard ASGI request.

Key observations from `src/ts/src/ipc/asgi.ts`:

- `handle()` reads the entire request body if the requester does not support streaming (`requestStream`), by calling `request.arrayBuffer()` and packaging a single `http.request` message.
- If the requester supports streaming, `handleStream()` is used, which breaks the body into chunks and sends multiple frames.
- The router match only influences `meta.type` and `handler_id`. There is no support for route-level capability or request encoding classification in this file.

On the Python side, `src/py/vertebra/server/worker.py` implements the ASGI worker. It builds an ASGI scope and provides a `receive()` coroutine backed by an internal queue. This is what Litestar expects: an async iterator yielding `http.request` events until `more_body` is false. The worker uses `RequestBodyState` to track `more_body` signals and manages the lifecycle of the request.

In the native pipeline, the Python worker still expects ASGI semantics, even when handling direct/native requests, because the worker ultimately runs the ASGI application (`self._app`) with `scope`, `receive`, and `send`. This is an important architectural detail: any approach that bypasses the standard ASGI `receive()` mechanism must either emulate it or replace Litestar’s parsing pipeline entirely.

## 4. Litestar Route Reflection and Router Build

Route reflection and native router construction are done in `src/py/vertebra/extensions/litestar/native.py`. This module walks `app.routes`, constructs `LitestarRoute` objects, and inserts route paths into a Rust-backed router with `Router.insert`. The path is translated from Litestar syntax into `matchit` syntax, and the route is keyed by method.

Relevant behaviors:

- `reflect_routes()` collects handlers, methods, guards, and dependencies.
- `build_router()` inserts `/{METHOD}{PATH}` into the router.
- There is no current capability classification or filtering at this stage.

This is the primary place where multipart capability awareness should be enforced, if the goal is to exclude multipart routes from native routing. The current spec `specs/active/native-pipeline-litestar-compat/` already outlines a route classification concept, and it should be extended or referenced to cover multipart explicitly.

## 5. Multipart Handling in ASGI and Litestar

Multipart parsing in Litestar depends on the ASGI `receive()` loop. The ASGI server (Uvicorn, Granian, etc.) delivers chunks of the request body via `receive()`, and Litestar’s request parsing consumes those chunks to decode boundaries and create `UploadFile` objects. This is precisely the flow described in the user’s document.

In the native pipeline, we do not have a raw socket stream; instead, Bun has already consumed the HTTP request, and the data is being relayed to Python via IPC. In order to preserve Litestar semantics, either:

- Bun must stream the body through IPC in a way that mimics ASGI `receive()` chunking, or
- The pipeline must bypass Litestar’s parser and produce a custom `UploadFile` representation.

Both paths have cost and risk. Streaming through IPC consumes ring buffer slots and may starve other messages. Replacing Litestar parsing requires a native multipart parser, plus a compatible file abstraction that is recognized by Litestar or by Vertebra’s extension layer.

## 6. Why Multipart Conflicts with Shared-Memory Rings

The IPC data plane is optimized for bounded messages. Ring buffer slots are fixed-size, and pushing large payloads means either chunking (which adds overhead and synchronization complexity) or allocating out-of-band buffers (which complicates ownership and lifecycle).

The problems outlined in the user document are real and can be tied to core design rules from `docs/arch.md` and `.claude/skills/ipc/SKILL.md`:

- Ring buffers are fixed-size and tuned for small messages.
- The broker is a shared resource for all requests; a large file upload can monopolize buffer slots.
- Zero-copy is the primary goal; pushing large byte streams through IPC threatens that goal.

In addition, the Bun broker uses fairness scheduling for pool requests. While multipart uploads are not database queries, the same ring infrastructure is shared, so large payloads could still cause scheduling delays and interfere with latency-sensitive control messages.

## 7. Existing Specification Notes

The existing spec `specs/active/native-pipeline-litestar-compat/enhancements.md` already documents the current multipart limitation and the rationale for ASGI fallback. This should be treated as a precursor or supporting reference for this PRD. The spec notes that:

- Multipart routes should be excluded from the native router.
- Bun should fall back to ASGI for these routes.
- The alternative (native multipart) requires new IPC opcodes and a streaming parser.

This PRD should be consistent with those statements while providing a deeper technical rationale and a more precise acceptance criteria for the fallback behavior.

## 8. Potential Architectural Approaches

### 8.1 Status Quo: ASGI Fallback

This approach keeps the native pipeline limited to JSON-style request bodies. Multipart requests are treated as unsupported and therefore routed through the ASGI path. This yields correctness and avoids destabilizing the data plane. The main downside is that multipart uploads do not benefit from the native pipeline’s lower latency. However, for large file uploads, network I/O tends to dominate, and the relative gain is minimal.

### 8.2 IPC Streaming via Chunked Frames

A native multipart implementation would likely require a new IPC opcode, such as `STREAM_CHUNK` or `http.request.stream`, enabling Bun to stream chunks into Python. This would mean:

- Bun reads the incoming request body in chunks and sends them via IPC frames.
- Python simulates ASGI `receive()` by pulling from the IPC queue and yielding chunks to Litestar.
- The worker must support backpressure and cancellation, and ensure that IPC frames do not starve control messages.

The risks are high: more complex scheduling, possible ring buffer saturation, and a need to implement flow-control semantics across processes. It also introduces CPU and synchronization overhead in both Bun and Python.

### 8.3 Side-Channel Transfer (Temp Files or Large SHM)

Instead of sending multipart bytes through the ring buffers, the broker could write the file to a temporary file or a large shared-memory region, then send a lightweight handle to Python. This design would:

- Preserve ring buffers for control messages and metadata.
- Allow streaming to disk or large SHM while the upload is in progress.
- Require a new metadata protocol to communicate file handles, offsets, and cleanup responsibilities.

However, it introduces a new storage responsibility for the Bun broker, including cleanup, quotas, security, and cross-platform file semantics. It also changes the contract with Litestar, which expects an ASGI stream rather than a finished file handle. That means Python would need to translate the handle into a `UploadFile`-like object or bypass Litestar’s parser.

### 8.4 Rust Streaming Multipart Parser

A future design might place multipart parsing in `vertebra-core`, parsing boundaries in a streaming fashion and producing clean, file-backed or memory-mapped outputs. Rust could operate on raw socket data or data provided by Bun, but it still requires a streaming source. It would also need to produce compatible results for Python, likely by returning file descriptors or memory maps plus metadata about form fields.

This approach could be the most performant and reusable across languages, but it has the highest engineering cost and the most integration risk. It would also require careful cross-platform handling for file descriptors and memory maps.

## 9. Backpressure, Fairness, and Control Plane Safety

The fundamental risk in native multipart support is that large uploads could interfere with the low-latency control plane. The `docs/arch.md` and IPC skill emphasize bounded queues and backpressure. If multipart streaming is allowed to dominate ring buffers, requests like health checks or small API calls could be delayed.

A safe implementation would need explicit policies:

- Separate ring buffers or distinct channels for streaming payloads.
- Hard caps on concurrent multipart uploads.
- Backpressure from Python to Bun to slow or pause reads.
- Graceful cancellation when a client disconnects.

These policies are currently not in place in the native pipeline, which makes the ASGI fallback path the safest solution for Phase 1.

## 10. Security, Resource Management, and Operational Risk

Multipart uploads are a common vector for resource exhaustion. A native multipart path would need to handle:

- Disk space usage if files are buffered to disk.
- Memory usage if large SHM buffers are allocated.
- Ownership and cleanup of temporary files or SHM regions.
- File descriptor limits, especially on macOS and Windows.
- Path traversal and sanitization if any file naming is influenced by user input.

The ASGI fallback path delegates these concerns to the ASGI server and framework, which already have established controls and best practices.

## 11. Testing and Observability Requirements

The PRD should specify tests that assert multipart routes are excluded from native routing. The existing spec already recommends a route capability matrix. This PRD should add explicit test cases, such as:

- A Litestar endpoint that declares `RequestEncodingType.MULTI_PART` or accepts `UploadFile`, and confirm it is not inserted into the native router.
- Integration tests that send multipart requests and confirm the ASGI path is taken.
- Logging or metrics that report the reasons for fallback (e.g., `multipart`), so maintainers can see why a route was excluded.

These tests should live alongside existing ASGI integration tests in `src/py/tests` and Bun integration tests in `src/ts/tests`.

## 12. Documentation and PRD Alignment

This PRD must be consistent with the existing `native-pipeline-litestar-compat` spec and with the architecture statements in `docs/arch.md`. It should also note updates for future documentation, such as the native pipeline compatibility matrix. The PRD should mention any patterns discovered that are candidates for skill extraction, such as route capability classification or fallback reasons.

## 13. Conclusion of Research

The evidence from the repository and existing specs supports the statement that multipart uploads are not currently compatible with the native pipeline. The simplest and safest path is to formalize ASGI fallback for multipart routes in Phase 1. The research also identifies possible future work for native multipart support, but it should be treated as a separate, high-cost effort requiring new IPC primitives, side-channel data transfer, and potentially Rust-based parsing. The PRD should therefore focus on ensuring fallback is correct, explicit, and testable, while documenting the longer-term roadmap for a true native multipart pipeline.
