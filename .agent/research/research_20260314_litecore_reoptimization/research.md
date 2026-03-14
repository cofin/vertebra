# Research: Litecore Re-Optimization Review

## Executive Summary

- **Primary conclusion:** `litecore` should be treated as the strongest prior art for the HTTP hot path, routing, asset serving, type bridge, and benchmark discipline, but **not** as proof that every request in `vertebra` should traverse shared-memory workers.
- **Architectural implication:** The current `vertebra-next` plan overweights the shared-memory/worker foundation for general HTTP handling. `litecore` evolved in the opposite direction: it kept moving more of the hot path into a native Rust HTTP runtime and eventually cancelled its IPC-heavy direction.
- **Best donor branch:** `feat/mvp-v3-post` is the branch that matters most. Earlier branches are useful for understanding dead ends and transitional experiments, but `feat/mvp-v3-post` contains the clearest signal on what survived contact with performance work.
- **Recommended optimization direction for `vertebra`:** keep shared memory where it is genuinely a data-plane advantage (broker/resource calls, large payloads, structured IPC), but replace or de-emphasize the current HTTP-serving path with a more `litecore`-style direct Rust/PyO3 request path for ordinary HTTP traffic.
- **Unfinished work in `litecore`:** there are still clear gaps in native bundler DX, HMR wiring, JS runtime resolution, Arrow storage support, and subinterpreter mode. `litecore` is valuable as prior art, but it is not a fully finished donor repo.

## Codebase Analysis

### Current `litecore` architecture

The current `litecore` codebase is centered on:

- A Rust-first HTTP core with PyO3 bridging and multiple request handling modes.
- Native routing and batching inside `litecore-http`.
- A native bundler crate and Python orchestration around frontend/dev workflows.
- A TypeBridge system that now emits multiple artifacts (`types`, `routes`, `zod`, `valibot`, `openapi`) and HMR metadata.
- A growing framework surface: plugins, jobs, auth, MCP, logging, runtime JS support, and benchmark tooling.

Relevant sources:

- `/home/cody/code/c/litecore/README.md`
- `/home/cody/code/c/litecore/docs/mintlify/pages/internals/architecture.mdx`
- `/home/cody/code/c/litecore/src/rs/litecore-http/src/lib.rs`
- `/home/cody/code/c/litecore/src/rs/litecore-http/src/router.rs`
- `/home/cody/code/c/litecore/src/py/litecore/framework/codegen/_bridge.py`
- `/home/cody/code/c/litecore/docs/assets/type-bridge-v2.md`

### What `litecore` does well today

1. **Direct HTTP hot path**
   `litecore-http` already owns routing, dispatch, batching, compression, access logging, and optional Rust-native handling before Python dispatch. This is the clearest evidence that a direct Rust/PyO3 path is the right optimization target for plain HTTP traffic.

2. **Native routing**
   `litecore` has already implemented a native router and integrated it into request batching/dispatch. This is much closer to a production-ready routing strategy than the still-speculative `vertebra-engine` route in the current repo.

3. **Frontend/type tooling**
   The bundler, virtual-module flow, and TypeBridge v2 work are materially ahead of `vertebra`'s current state. The code shows a coherent story around generated types, route artifacts, schema artifacts, and dev-time update signaling.

4. **Benchmark culture**
   The later branches add TFB infrastructure, validation tooling, performance gates, profiling flows, and PGO. Even when the architectural direction changed, the project kept measurement in the loop.

### What `litecore` does not provide

`litecore` does **not** currently expose the shared-memory IPC substrate assumed by `vertebra-next` Phase 1. The Rust workspace is:

- `litecore-bundler`
- `litecore-http`
- `litecore-js`
- `litecore-storage`

It does not contain the `shm` / `SpscRing` / `MpmcRing` / worker-manager foundation that the `vertebra-next` plan claims to port from `litecore`.

This means the current plan is using `litecore` more as an inspiration label than as an actual code donor for Phase 1.

## Prior Art

### Branch evolution in `litecore`

#### `feat/mvp`

This is the original "Rust HTTP runtime plus protocol bridges" branch.

- Introduced the first PyO3-backed server runtime.
- Added RSGI, ASGI, and WSGI bridging.
- Added a benchmark harness.
- Added a full zero-copy/shared-memory worker path and related tests.

Interpretation:

- This branch proves `litecore` seriously explored the same kind of shared-memory worker architecture now being re-proposed in `vertebra-next`.
- It is important mainly because later branches moved away from this being the center of gravity.

#### `feat/mvp-2`

This branch is an architectural reboot.

- Archived the v1 IPC direction.
- Introduced a native-core rewrite direction.
- Added the first Type Bridge system.
- Added the first native Rolldown/PyO3 bundler work.
- Expanded build and packaging infrastructure.
- Added Docker benchmark infrastructure and Cloud Run H2C support.

Interpretation:

- This is where `litecore` stopped treating the shared-memory worker path as the main answer and shifted toward "native core + tooling + integration".

#### `feat/mvp-3`

This is the bridge-tax and codegen/HMR branch.

- Batched dispatch to reduce Python bridge overhead.
- Added extractor caches and digest caches.
- Expanded type extraction across TypedDict/dataclass/Pydantic/msgspec/generics.
- Added watch/HMR lifecycle and virtual module support.
- Tightened benchmark validation.

Interpretation:

- This branch is the strongest prior art for "reduce Python boundary cost without forcing every request through multi-process shared memory".

#### `feat/mv-v3-pre-asgi`

This branch is mostly native bundler mode and DX parity work.

- Normalized ESM/TypeScript config.
- Added native bundler mode.
- Reworked templates and dev wiring.
- Added CI/CD and PRDs for future ASGI/WSGI/framework work.

Interpretation:

- This is groundwork, not the final runtime answer.

#### `feat/mvp-v3-post`

This is the most important branch and the current best donor.

- Added ASGI runtime redesign and hot-path performance work.
- Added WSGI redesign.
- Added Rust asset server and dev proxying.
- Added framework adapters, auth, plugins, jobs, logging, MCP, gRPC, and JS runtime work.
- Added TypeBridge v2 artifacts and HMR metadata.
- Added a batch-first redesign where `litecore` is no longer fundamentally "just ASGI".
- Added TFB, PGO, native routing, and database/plugin work.
- Archived/cancelled `litecore-ipc`.

Interpretation:

- `litecore` did not converge on "shared memory for all HTTP".
- It converged on "native Rust HTTP + targeted bridges + native routing + better codegen/tooling + aggressive profiling".

### Implication for `vertebra`

The branch history argues for a different optimization strategy than the current `vertebra-next` plan:

- **Do not** assume shared-memory workers are the correct general HTTP path.
- **Do** use shared memory selectively where the data plane benefits.
- **Do** prioritize native routing, lazy request materialization, response serialization optimization, direct PyO3 execution, and benchmark gates.

## Library Documentation

### PyO3

Relevant official docs:

- PyO3 user guide, free-threaded Python: https://pyo3.rs/main/free-threading.html
- PyO3 user guide, parallelism and `detach`: https://pyo3.rs/main/parallelism.html
- `pyo3-async-runtimes` asyncio integration guide: https://github.com/PyO3/pyo3-async-runtimes

Key takeaways:

- PyO3's current guidance is to use `Python::attach`/`Python::detach` patterns rather than older GIL terminology in the hot path.
- Free-threaded Python support is real, but extension authors must be explicit about thread-safety expectations.
- Long-running or blocking work should detach from the Python interpreter state instead of holding it across waits.
- Async integration remains a tricky area when a Rust runtime is driving Python coroutines; event-loop lifecycle and cross-thread execution are still the main risk zone.

### Hyper and Tokio

Relevant official docs:

- Hyper guides index: https://hyper.rs/guides/1/
- Hyper server hello-world guide: https://hyper.rs/guides/1/server/hello-world/
- Hyper graceful shutdown guide: https://hyper.rs/guides/1/server/graceful-shutdown/

Key takeaways:

- The official Hyper/Tokio guidance still matches the accept-loop plus `TokioIo`/service-per-connection model already visible in both repos' experiments.
- Hyper is a solid basis for a direct Rust HTTP server, but graceful shutdown, connection lifecycle, and backpressure policy need explicit ownership rather than being incidental side effects of the Python layer.

### `notify` / `notify-debouncer-mini`

Relevant official docs:

- `notify` docs: https://docs.rs/notify/latest/notify/
- `notify-debouncer-mini` docs: https://docs.rs/notify-debouncer-mini/latest/notify_debouncer_mini/

Key takeaways:

- Rust-native file watching is a reasonable replacement for Python `watchdog`, but the official docs still call out cross-platform/watch-backend caveats.
- Polling fallbacks may still be required in Docker, network FS, or platform-specific edge cases.
- This supports `litecore`'s direction of replacing Python HMR watching with a Rust watcher, but it should be treated as a reliability upgrade project, not as "drop in and forget".

## Replacement Analysis For `vertebra`

If `vertebra` were re-optimized around what `litecore` actually learned, the main replacements or de-emphases would be:

### 1. Replace the general-purpose HTTP serving path

Most likely replacement targets:

- `/home/cody/code/c/vertebra/src/rs/vertebra-server`
- `/home/cody/code/c/vertebra/src/py/vertebra/server`
- `/home/cody/code/c/vertebra/docs/architecture/native-integration.md`

Reason:

- `vertebra` currently treats the broker/server plus worker IPC architecture as the canonical serving model.
- `litecore` provides stronger prior art for a direct Rust HTTP core with PyO3 bridging, native routing, and lazy request/response materialization for ordinary HTTP traffic.

Recommendation:

- Recast shared memory as an optimization for broker/resource/data operations, not the default transport for every HTTP request.

### 2. De-emphasize the assumption that `vertebra-engine` Phase 1 should lead the rewrite

Most likely replacement or scope-reduction target:

- `/home/cody/code/c/vertebra/src/rs/vertebra-engine`
- `/home/cody/code/c/vertebra/.agent/specs/vertebra-next/plan.md`

Reason:

- The current plan starts with a shared-memory worker foundation.
- `litecore`'s later branches suggest the better order is the reverse: first optimize direct HTTP/native routing, then retain IPC only where it still wins.

Recommendation:

- Re-scope `vertebra-engine` into a narrower component:
  - keep embedded execution experiments
  - keep routing/serialization ideas
  - stop assuming the shared-memory worker substrate is the universal base layer

### 3. Replace route reflection and dispatch strategy with a native-routing-first design

Most likely target files:

- `/home/cody/code/c/vertebra/src/rs/vertebra-server/src/router.rs`
- `/home/cody/code/c/vertebra/src/rs/vertebra-server/src/dispatcher.rs`
- `/home/cody/code/c/vertebra/src/rs/vertebra-server/src/response_router.rs`
- `/home/cody/code/c/vertebra/src/py/vertebra/core/worker.py`

Reason:

- `litecore`'s native routing and route-id handoff work is more mature and more directly performance-driven than the current `vertebra` route of "broker dispatch everything through workers".

Recommendation:

- Move toward native route matching with direct handler metadata lookup and a smaller Python boundary.

### 4. Replace or absorb frontend/type tooling with the `litecore` model

Most likely target files:

- `/home/cody/code/c/vertebra/src/py/vertebra/core/codegen/typescript.py`
- `/home/cody/code/c/vertebra/src/py/tests/unit/codegen/test_type_bridge.py`

Reason:

- `litecore` has already grown beyond basic type generation into route/schema/openapi artifacts, HMR metadata, virtual modules, and bundler coordination.

Recommendation:

- Treat `litecore`'s TypeBridge v2 and bundler flow as the reference model for future `vertebra` type-bridge work.

### 5. Keep the broker and shared-memory core, but narrow their role

Most likely survivors:

- `/home/cody/code/c/vertebra/src/rs/vertebra-core`
- `/home/cody/code/c/vertebra/src/rs/vertebra-broker`
- `/home/cody/code/c/vertebra/src/py/vertebra/messaging/transport`
- `/home/cody/code/c/vertebra/src/py/vertebra/database/backends/broker.py`

Reason:

- `litecore` does not supersede `vertebra`'s database/resource broker story.
- It does, however, strongly suggest that the broker should not be the default path for ordinary HTTP request handling.

Recommendation:

- Keep shared memory and broker transport for:
  - pooled DB/storage/resource access
  - large structured payloads
  - broker-specific acceleration services
- Do not require every inbound HTTP request to pay that architecture cost.

## Unfinished Or Stubbed Areas In `litecore`

### High confidence unfinished

1. **Native bundler dev path still has a missing implementation path**
   - `/home/cody/code/c/litecore/src/py/litecore/cli/js/assets.py`
   - Native mode still exits with "Native bundler not yet implemented" on import failure.

2. **HMR placeholder path still exists**
   - `/home/cody/code/c/litecore/src/py/litecore/framework/dev/_middleware.py`
   - There is still a WebSocket placeholder that accepts then closes with "HMR not implemented yet".

3. **`npm:` resolution is still not implemented in the Rust JS loader**
   - `/home/cody/code/c/litecore/src/rs/litecore-js/src/modules/loader.rs`

4. **Arrow methods in SQLSpec storage are still stubs**
   - `/home/cody/code/c/litecore/src/py/litecore/framework/plugins/sqlspec/storage.py`

5. **Subinterpreter mode is explicitly not implemented**
   - `/home/cody/code/c/litecore/src/py/litecore/server/_native_import.py`

### Medium confidence unfinished

6. **Python HMR watcher is intentionally temporary**
   - `/home/cody/code/c/litecore/src/py/litecore/framework/dev/_watcher.py`

7. **The Python module server is intentionally simple and transitional**
   - `/home/cody/code/c/litecore/src/py/litecore/framework/dev/_module_server.py`

8. **The build orchestrator still emits placeholder generated types**
   - `/home/cody/code/c/litecore/src/py/litecore/framework/bundler/build_orchestrator.py`

### Low confidence or cosmetic unfinished

9. **JS console ops still bypass Python logging**
   - `/home/cody/code/c/litecore/src/rs/litecore-js/src/ops/console.rs`

10. **CLI banner version remains placeholder-ish**
   - `/home/cody/code/c/litecore/src/py/litecore/cli/banner/renderer.py`

## Risk Assessment

### Technical risks

1. **Plan drift risk**
   The biggest risk is continuing to invest in `vertebra-next` as if `litecore` validates the Phase 1 shared-memory-first strategy, when the `litecore` branch history shows the opposite trend.

2. **Partial transplant risk**
   Lifting only isolated `litecore` optimizations without adopting its routing/request/response ownership model would likely produce extra complexity without the same performance payoff.

3. **Async runtime complexity**
   PyO3 plus Rust async plus Python coroutine execution is still a sharp edge, especially for background loops, cross-thread execution, and shutdown ordering.

4. **Dev tooling portability**
   Replacing Python watchers with Rust `notify` infrastructure is directionally correct, but file watching remains platform-sensitive and needs reliability work, not just speed work.

5. **Overcorrection risk**
   It would be a mistake to throw out all broker/shared-memory work. That substrate still looks valuable for `vertebra`'s broker/resource/data-plane responsibilities.

### Recovery strategy

- Re-scope the `vertebra-next` work before deeper implementation.
- Build a benchmarkable direct HTTP serving slice first.
- Preserve shared-memory broker/data-plane work as an independent capability.
- Keep a fallback proxy/ASGI path during migration.
- Adopt `litecore`'s benchmark-gate mindset early so architectural debates are resolved with measurements, not preference.

## Recommended Approach

### Short version

Use `litecore` as the reference architecture for HTTP hot-path design, not as a reference for universal shared-memory request handling.

### Recommended next steps

1. **Revise the `vertebra-next` PRD/plan**
   Reframe Phase 1 around direct HTTP serving, native routing, lazy request materialization, and response serialization.

2. **Limit the role of shared memory**
   Keep it for broker/resource/data-plane acceleration, not for every inbound HTTP request.

3. **Study `feat/mvp-v3-post` first**
   That branch has the most relevant code and the clearest evidence of what survived multiple rounds of performance work.

4. **Lift these ideas first**
   - native router plus route-id dispatch
   - lazy request wrappers
   - pre-compiled response serialization
   - Rust asset/static serving
   - richer type bridge and virtual module flow
   - performance gates / TFB / profiling tooling

5. **Treat these as incomplete donor areas**
   - native bundler dev server
   - HMR end-to-end replacement of Python watcher/middleware placeholders
   - subinterpreter runtime path
   - `npm:` JS runtime resolution
   - Arrow support in storage

### Final judgment

If the goal is "what should `vertebra` steal from `litecore`?", the answer is:

- **Steal the HTTP/runtime/routing/tooling ideas**
- **Keep `vertebra`'s broker/shared-memory advantages where they actually pay off**
- **Stop assuming the whole product should be rebuilt around shared-memory workers for all requests**
