# PRD: Multipart Uploads and ASGI Fallback in the Native Pipeline

## 1. Intelligence Context

### 1.1 Complexity Assessment

**Complexity: Complex**

This effort crosses multiple layers of the Vertebra stack (Bun server, shared-memory IPC, Python ASGI worker, and Litestar request handling). It touches routing policy, request parsing semantics, and operational safety for large payloads. Even though Phase 1 preserves the existing ASGI fallback strategy, the PRD must define a precise capability classification rule, document the architectural rationale, and specify tests and observability. The future-native path (streaming IPC + side-channel transfer + Rust parsing) is high-cost and must be documented as a deferred roadmap item. This is a cross-language architecture decision with significant impact on correctness and performance guarantees.

**Checkpoint count:** 12

### 1.2 Detected Stack and Commands

**Repository:** `vertebra` (current working repo)

**Primary languages:** Python 3.12+, TypeScript (Bun), Rust

**Build/Test commands (from `AGENTS.md`, `README.md`, `CLAUDE.md`):**

- `make install` - full setup (Python + Bun + Rust)
- `make test` - Python tests
- `make js-test` - Bun/TypeScript tests
- `make rs-test` - Rust tests
- `make test-all` - all tests across Rust, TS, Python
- `make lint` / `make lint-all` - format + lint across the stack
- Integration tests: `uv run pytest src/py/tests -m integration --integration`

**Docs system:** No Sphinx or MkDocs detected. Documentation lives in Markdown under `docs/` and specs live in `specs/active/`.

**PRD location:** `specs/active/native-multipart-fallback/`

### 1.3 Similar Implementations Found

1. `src/py/vertebra/extensions/litestar/native.py` - Litestar route reflection and native router insertion.
2. `src/ts/src/ipc/asgi.ts` - Bun-side ASGI bridge, direct routing, and request streaming.
3. `src/py/vertebra/server/worker.py` - ASGI worker, `receive()` semantics, and response framing.
4. `src/ts/src/server/index.ts` - Bun server configuration, AsgiBridge wiring, router setup.
5. `specs/active/native-pipeline-litestar-compat/enhancements.md` - Existing multipart fallback rationale.

### 1.4 Patterns to Follow

- **Capability-first routing:** Insert only compatible routes into the native router and let everything else fall back to ASGI.
- **ASGI correctness over native performance:** If request parsing or response semantics are uncertain, preserve ASGI behavior.
- **Shared-memory data plane discipline:** Keep ring buffers for bounded, low-latency traffic; avoid streaming large payloads through them.
- **Explicit logging and testability:** Fallback reasons should be deterministic and auditable via tests and startup logs.

---

## 2. Problem Statement

Vertebra’s native pipeline is optimized for zero-copy, small-payload request/response flows. The shared-memory ring buffers and Bun broker architecture are explicitly designed for bounded messages such as JSON or MessagePack payloads. Multipart/form-data file uploads do not fit this model: they are unbounded in size, must be streamed, and produce multi-part parsing semantics that Litestar expects to handle via the ASGI `receive()` loop.

In a standard ASGI server, the raw socket stream is exposed to the application as a stream of chunks via `receive()`. Litestar uses those chunks to parse multipart boundaries and to build `UploadFile` abstractions. The native pipeline bypasses this ASGI receive loop; Bun receives the HTTP stream and then relays data to Python via shared-memory IPC frames. This makes multipart parsing incompatible with the native path unless we either emulate `receive()` with streamed IPC chunks or replace Litestar’s parser with a native multipart implementation.

The consequences of native multipart handling are significant:

- **Arbitrary payload size:** File uploads can be gigabytes, far exceeding the size of ring buffer slots.
- **Streaming requirement:** File uploads must be streamed; buffering entire payloads in RAM or SHM is unsafe.
- **Broker bottleneck:** Pushing large file data through ring buffers will saturate the IPC channel, delaying control messages, health checks, and latency-sensitive API calls.

Given these constraints, the safest and most correct approach today is to explicitly exclude multipart routes from the native router and allow the request to flow through the ASGI fallback path. This preserves correctness, avoids resource starvation in the shared-memory data plane, and aligns with current architecture guidelines.

### 2.1 User Problem and Business Value

**User problem:** Maintainers need multipart file uploads to work reliably when using `vertebra serve`. If multipart routes are accidentally routed through the native pipeline, they may fail or degrade the performance of the entire system. The native pipeline should never break file uploads, and it should not allow uploads to block unrelated API requests.

**Business value:** The fallback strategy preserves correctness and system stability. It allows the native pipeline to deliver low-latency JSON APIs without being compromised by large uploads. It also prevents complex and high-risk engineering work from being rushed before supporting infrastructure exists.

### 2.2 Success Criteria

- Multipart routes (explicit `RequestEncodingType.MULTI_PART` or `UploadFile` dependencies) are excluded from the native router.
- Requests to those routes are always routed through the ASGI bridge.
- The native pipeline continues to handle small JSON requests efficiently without interference.
- Developers can understand and verify fallback decisions through logs and tests.

### 2.3 Non-Goals

- Implementing a full native multipart parser in Phase 1.
- Introducing a streaming IPC protocol for large payloads in Phase 1.
- Changing Litestar’s request parsing semantics.
- Providing a production-ready side-channel upload system in Phase 1.

### 2.4 Stakeholders and User Stories

**Primary stakeholders** are the maintainers of Vertebra, the Bun broker, and the Litestar integration layer. Secondary stakeholders are teams building applications on top of Vertebra who depend on uploads working in development and production.

**User stories** clarify the expected behavior:

1. **Maintainer running a real app with uploads**
   As a maintainer, I run an application with multipart uploads using `vertebra serve`. I expect file uploads to work exactly as they do under pure ASGI and never to be routed through a partial or experimental native path.

2. **Maintainer validating native performance**
   As a maintainer, I want the native pipeline to remain fast and responsive for JSON APIs. Large uploads should not reduce throughput or latency for unrelated requests.

3. **App developer troubleshooting routing behavior**
   As an app developer, I want to understand why a route falls back to ASGI. Logs should clearly indicate that a route was excluded for `multipart` so I can reason about performance expectations.

4. **Operator monitoring system health**
   As an operator, I need health checks and small control-plane endpoints to remain responsive even when a large upload is in progress. The architecture must avoid head-of-line blocking in shared memory.

5. **Future maintainer extending native support**
   As a future maintainer, I want a roadmap and clear constraints documented so that any decision to implement native multipart support is informed by the existing trade-offs and architecture rules.

### 2.5 Constraints and Assumptions

- **Ring buffer size is fixed and bounded.** The shared-memory data plane is designed for small messages, not large byte streams. Any attempt to stream large payloads through the existing rings risks starving control traffic.
- **ASGI semantics are the compatibility baseline.** The Python worker runs Litestar via ASGI scope and `receive()`/`send()` calls. Any native multipart implementation must either emulate this behavior or replace it entirely.
- **The Bun broker is a shared resource.** Multipart uploads cannot monopolize the broker or ring buffers without violating latency guarantees for other requests.
- **Cross-platform parity is required.** Any future native solution must work on Linux, macOS, and Windows. File descriptors or memory mapping APIs must be abstracted accordingly.
- **Correctness outweighs micro-optimization.** For large uploads, network I/O dominates latency, so the benefit of native handling is minimal compared to the risk.

---

## 3. Acceptance Criteria

### 3.1 Routing and Compatibility

- Any route that declares `RequestEncodingType.MULTI_PART`, a string literal media type of `"multipart/form-data"`, or accepts `UploadFile` (including `Optional[UploadFile]`, `list[UploadFile]`, or nested dependency injections) must be classified as **ASGI fallback**.
- Multipart routes are not inserted into the shared-memory router (`Router.insert`), ensuring Bun never dispatches them as `direct.http`.
- The classification must be deterministic at startup and reproducible in tests.

### 3.2 Correctness and Safety

- Multipart uploads must work identically to the ASGI-only path.
- No multipart upload should flow through shared-memory ring buffers as a large byte stream.
- Control-plane traffic (health checks, metadata requests, small APIs) must not be delayed by multipart uploads.
- If a request matches a native route but presents `Content-Type: multipart/form-data` or a payload size above the native body threshold, Bun must force ASGI fallback (or return a clear 415/413 error) rather than attempting `request.arrayBuffer()` on the full body.

### 3.3 Observability

- Startup logging must summarize how many routes are native vs fallback, with explicit reasons such as `multipart`.
- Route-classification logs should be structured and include `route_path`, `http_method`, `handler_id`, `classification`, and `fallback_reason`.
- Logs should be emitted at a consistent level (e.g., `info`) and be JSON-serializable to support automated parsing.
- A counter metric (e.g., `vertebra_multipart_fallback_total`) must be emitted whenever a multipart route is forced to ASGI.
- Optional debug logging should allow inspection of route classification for a specific handler.

### 3.4 Testing

- Unit tests confirm route classification for multipart handlers.
- Unit tests cover string media types (`"multipart/form-data"`), nested `UploadFile` dependencies, and controller/app-level config inheritance.
- Unit tests cover multi-method routes where one method implies multipart and another does not.
- Integration tests send multipart requests and confirm the ASGI path is used.
- Integration tests assert that multipart requests do not emit `direct.http` metadata in IPC frames.
- Integration tests verify the runtime Bun guard for `Content-Type: multipart/form-data` on a native-matched route.
- Regression tests ensure multipart routes do not appear in the native router’s route map.
- Negative tests confirm that misclassified native routes fail fast with a clear error rather than buffering large payloads.

### 3.5 Documentation and Skills

- Architecture documentation must describe the multipart fallback behavior and its rationale.
- Any newly discovered patterns (route capability classification, fallback reasons) should be recorded for future skill extraction.

### 3.6 Edge Cases and Error Scenarios

- **Client disconnect mid-upload:** The ASGI fallback path must continue to propagate disconnect semantics to Litestar so uploads abort cleanly and do not leak file handles or buffers.
- **Multiple files in one request:** Multipart requests with several files should be treated the same as single-file uploads, and all must go through ASGI fallback.
- **Mixed form fields and files:** Requests containing both small fields and files should not be partially routed; the entire request must be handled by ASGI.
- **Invalid boundaries or malformed bodies:** These should be handled by Litestar’s standard error handling, not by the native pipeline.
- **Missing or malformed Content-Type:** If the request is routed to an upload handler but the header is missing or malformed, the request must still be served via ASGI and produce the same errors as standard Litestar behavior.
- **Chunked transfer encoding:** Multipart uploads using `Transfer-Encoding: chunked` must route through ASGI and preserve streaming semantics.
- **Large headers or metadata:** Oversized metadata should not affect the native router; if a route is multipart, it falls back regardless of header size.

### 3.7 Performance and SLOs

- Under concurrent multipart upload load, health-check endpoints (e.g., `/__health` or `/health`) must maintain **p99 latency <= 100ms** on local integration tests or CI baseline machines.
- Routing classification must not add more than **50ms** to application startup time for a representative app.

---

## 4. Technical Approach

### 4.1 High-Level Strategy

Phase 1 is a policy and correctness phase. It does not attempt to implement native multipart handling; instead, it formalizes and tests the ASGI fallback behavior for multipart uploads. The goal is to protect the shared-memory data plane and ensure user expectations are met.

The strategy is:

1. **Detect multipart requirements** during route reflection or compilation.
2. **Mark those routes as fallback** and exclude them from the native router.
3. **Allow Bun to route unmatched requests** to the ASGI bridge, which already supports streaming and multipart parsing via standard ASGI semantics.

### 4.2 Route Classification

The most appropriate location for multipart classification is the route reflection/compilation stage in Python. `src/py/vertebra/extensions/litestar/native.py` already reflects routes and collects metadata, while `build_router()` inserts into the shared-memory router. A classifier should check the route handler metadata for:

- Explicit `Body(media_type=RequestEncodingType.MULTI_PART)` declarations.
- Dependencies or parameters typed as `UploadFile`.
- Any explicit `multipart/form-data` media types in the handler signature or route configuration.

If any of these are present, the route should be marked as fallback and skipped during router insertion.

Because Litestar handlers can be defined in controllers and can inherit configuration, the classifier should inspect the resolved handler configuration, not just raw annotations. This includes string-based media types (e.g., `Body(media_type="multipart/form-data")`) and layered configuration from app, router, controller, and handler levels. This may require using Litestar’s handler APIs rather than only `handler.fn`.

### 4.3 Bun Routing Behavior

Bun’s `AsgiBridge` (`src/ts/src/ipc/asgi.ts`) performs a router match and sets `meta.type = "direct.http"` when a match exists. The fallback behavior relies on the router not matching multipart routes. Therefore:

- It is crucial that multipart routes are excluded from the router on the Python side.
- Bun must enforce a runtime safety guard: if `Content-Type` is multipart or the body size exceeds the native threshold, force ASGI fallback (or fail fast with a clear 415/413), even if the router matched natively.

This preserves a clean separation of responsibilities: Python decides which routes are safe for native routing, and Bun routes accordingly.

### 4.4 Bun Streaming Path Clarification

`AsgiBridge` already exposes `handleStream()` for streaming ASGI bodies. This PRD clarifies that multipart uploads continue to use the **ASGI streaming path**, not the native `direct.http` path. If Bun determines a request should fall back (by classification or runtime guard), it must use the same streaming semantics that ASGI expects, preserving backpressure and chunk ordering.

### 4.5 Python ASGI Worker and Multipart Parsing

The Python worker (`src/py/vertebra/server/worker.py`) already builds an ASGI scope and implements `receive()` and `send()` to support streaming bodies. This is the correct and tested path for multipart requests. There is no need to modify the worker for Phase 1.

### 4.6 Capability Matrix and Classification Metadata

The native pipeline should expose an internal capability matrix that associates route handlers with the reasons they are excluded. This does not need to be a user-facing API in Phase 1, but it should exist in logs and tests. The matrix should include at least the following fields per route:

- route path and method
- handler identifier
- classification: `NativeSupported` or `AsgiFallback`
- fallback reasons: `multipart`, `websocket`, `static`, `unsupported_di`, etc.

For multipart support, the classifier must report a clear `multipart` reason so maintainers can distinguish it from other fallback conditions. This also allows future expansion: if native multipart support is added later, tests can be updated to assert a change in classification.

### 4.7 Configuration and Policy Controls

Phase 1 does not require a new configuration flag, but the design should allow a future toggle to override fallback policy. If such a flag is introduced later, it must be default-off and must include a warning about potential data-plane saturation. For now, the classification is hard-coded and conservative.

### 4.8 Future Architecture (Deferred)

Native multipart support is deferred, but the PRD should document the future path:

- **Streaming IPC opcode:** Introduce a `STREAM_CHUNK` or equivalent to carry body fragments.
- **Side-channel transfer:** Allow Bun to write data to disk or large SHM, then pass handles to Python.
- **Rust streaming parser:** Implement multipart boundary stripping and form field extraction in `vertebra-core`.
- **Litestar compatibility layer:** Provide a `receive()` emulation or a custom `UploadFile` abstraction compatible with Litestar.

These are out of scope for Phase 1 but should be included as roadmap notes.

Phase 2 evaluation criteria should prioritize (in order): control-plane safety, cross-platform viability, operational complexity (cleanup/quotas), and implementation cost. The initial assumption is that side-channel transfer plus a Rust streaming parser is the most viable long-term option, but this must be validated with benchmarks and prototype results.

### 4.9 File Structure Plan

Potential files to update in a follow-on implementation phase (not executed in PRD creation):

- `src/py/vertebra/extensions/litestar/native.py` - add multipart capability detection and fallback tagging.
- `src/py/vertebra/extensions/litestar/compiler.py` - if route compiler needs to carry capability metadata.
- `src/py/vertebra/server/worker.py` - likely no changes for Phase 1, but referenced for compatibility.
- `src/ts/src/ipc/asgi.ts` - no direct changes expected for fallback, but ensure routing depends on router matches only.
- `src/ts/src/server/index.ts` - ensure router and bridge wiring remains unchanged.
- `src/py/tests/unit/` and `src/py/tests/integration/` - add tests for multipart fallback.
- `src/ts/tests/integration/` - add integration tests for ASGI fallback in Bun layer.

### 4.10 Database, API, and Frontend Impact

- **Database:** No schema changes.
- **API endpoints:** No new endpoints; only routing policy is affected.
- **Frontend:** No changes required.

### 4.11 Documentation Updates Required

Docs system is Markdown-only. Update or add sections in:

- `docs/arch.md` - add explicit mention of multipart fallback and its rationale.
- `docs/architecture/native-integration.md` - clarify the native router capability model and fallback rules.
- `docs/progress.md` - note Phase 1 decision and future roadmap for native multipart support.

### 4.12 Skills to Update

New patterns discovered for skill extraction:

- Route capability classification (native vs fallback) based on request encoding and handler dependencies.
- Fallback reasoning patterns for large payloads in shared-memory data planes.

These should be considered for updates to `~/.claude/skills/vertebra/` and `~/.claude/skills/ipc/`.

---

## 5. Testing Strategy

### 5.1 Unit Tests (90%+ Coverage for New Logic)

- Add unit tests around the route classification function to ensure multipart routes are tagged as fallback.
- Use small Litestar app fixtures that define `UploadFile` parameters and explicit `RequestEncodingType.MULTI_PART` bodies.
- Verify that only compatible routes are inserted into the native router.

### 5.2 Integration Tests

- End-to-end tests that spin up a Bun server and Python worker and send multipart requests.
- Confirm that these requests are routed via ASGI (e.g., no `direct.http` type in IPC metadata).
- Ensure that uploads succeed and do not block concurrent JSON requests.

### 5.3 Optional Stress Tests

- Simulate large uploads and verify that control-plane requests (health checks, small JSON calls) remain responsive.
- These tests can be added later but should be documented as a future verification step.

### 5.4 Commands to Run

- `make test` for Python unit tests.
- `make js-test` for Bun integration tests.
- `make test-all` for full cross-stack verification (optional in CI).

### 5.5 Observability and Manual Verification

- Validate that startup logs report the correct counts for native vs fallback routes, with explicit `multipart` reasons.
- Manually exercise a multipart endpoint in a dev app and confirm the request is served by the ASGI path (e.g., by inspecting IPC logs or tracing).
- Confirm that unrelated JSON endpoints remain low-latency while uploads are in progress.
- Verify the `vertebra_multipart_fallback_total` counter increments for multipart requests.

### 5.6 Performance and Resource Testing (Future)

While Phase 1 does not change performance characteristics, future work should include a benchmark that measures:

- The overhead of routing classification during startup.
- The impact of large uploads on ring buffer latency for unrelated traffic.
- Memory and file descriptor usage if a side-channel transfer approach is attempted.

---

## 6. Implementation Notes

### 6.1 Pattern Deviations

- The native router normally assumes route compatibility; this PRD formalizes a more conservative approach by explicitly excluding multipart routes. This is a deviation toward safety, justified by shared-memory constraints.

### 6.2 Dependencies and Migrations

- No new external dependencies are required for Phase 1.
- No database migrations.

### 6.3 Cross-Platform Considerations

- The fallback approach is inherently cross-platform because it leverages standard ASGI behavior.
- Any future native multipart support must be designed with Windows and macOS in mind, especially for file handles and memory mapping.

### 6.4 Risk and Mitigations

- **Risk:** Multipart routes are incorrectly classified and routed natively.
  - **Mitigation:** Add explicit unit tests and a route capability log at startup.
- **Risk:** Large multipart bodies are buffered into memory on the Bun side due to a misclassification or missing `Content-Type`.
  - **Mitigation:** Add the Bun runtime guard to force ASGI fallback or return 415/413 before buffering.
- **Risk:** Overly conservative classification routes more traffic through ASGI than necessary.
  - **Mitigation:** Provide explicit diagnostic logs and allow future incremental expansion.

### 6.5 Rollout Plan

- Phase 1 focuses on correctness and documentation.
- Future phases can explore IPC streaming and Rust parsing once foundational infrastructure is ready.

### 6.6 Alternatives Considered

**Alternative A: Stream multipart data through existing ring buffers.**
This approach was rejected because it violates the design goal of keeping ring buffer traffic small and bounded. Large uploads could fill ring slots, delay control-plane messages, and undermine tail latency for small requests. It would also require complex backpressure handling across processes.

**Alternative B: Use a large shared-memory region for uploads.**
This would require new allocation, ownership, and cleanup semantics and would still need streaming coordination between Bun and Python. It also creates cross-platform challenges for memory mapping and file descriptor transfer. The operational risk is high for Phase 1.

**Alternative C: Offload multipart parsing to Rust without a streaming protocol.**
Rust parsing is promising but still requires a data source. Without a streaming protocol from Bun or the raw socket, this approach cannot be implemented cleanly. It is deferred until a proper streaming IPC design is in place.

### 6.7 Open Questions

- What is the minimal streaming IPC protocol that preserves backpressure and does not starve the control plane?
- Should the side-channel transfer be disk-first (temp files) or memory-first (large SHM), and how should cleanup be handled?
- How should native multipart parsing surface `UploadFile` semantics to Litestar or to Vertebra’s extension layer?
- What is the right observability model for tracking multipart fallback, including counters and per-route diagnostics?
- Which real-world apps should be used as the canonical regression suite for multipart behavior?

---

## 7. Summary

This PRD formalizes the strategy that multipart/form-data requests are incompatible with the current native pipeline and must be routed through the ASGI fallback path. It defines clear acceptance criteria, a conservative route classification policy, and a test plan to guarantee correctness. It also documents the longer-term roadmap for native multipart support while explicitly deferring that work to future phases due to high complexity and limited performance benefit. The result is a safer, more predictable native pipeline that preserves zero-copy performance for small RPC-style requests and ensures that file uploads remain reliable.

This approach aligns with the existing native pipeline compatibility work and provides a stable baseline for future experimentation, without compromising the shared-memory data plane or developer expectations today.
