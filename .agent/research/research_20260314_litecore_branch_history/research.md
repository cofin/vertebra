# Research: Litecore Branch History and Architectural Evolution

## Executive Summary

- Yes, `litecore` did go through a real C++ phase. The clearest proof is the `feat/mvp-2` branch on February 1, 2026, where commit `e3a5734` explicitly archived the v1 Rust shared-memory/IPC code and reset the project for a v2 CFFI rewrite, followed immediately by `b9740e4` adding a uWebSockets-based C++ core and `ee08226` adding Python CFFI bindings.
- That C++/CFFI path did not survive. By February 3, 2026, `feat/mvp-3` had already pivoted the project back to a Rust-first HTTP core, and its head commit `bdd4014` explicitly rewrote the project docs and patterns around a Rust/PyO3 ownership model while archiving the CFFI-era plan.
- The larger branch history shows five distinct architectural eras:
  1. `feat/mvp`: PyO3 Rust server plus zero-copy shared-memory worker architecture.
  2. `feat/mvp-2`: C++/uWebSockets HTTP core plus Python CFFI, with Rust retained for the native bundler.
  3. `feat/mvp-3`: Rust-first HTTP core migration, bridge-tax elimination, type bridge, HMR, and benchmark gates.
  4. `feat/mv-v3-pre-asgi`: native bundler mode and DX parity groundwork.
  5. `feat/mvp-v3-post`: the current line, which adds framework APIs, ASGI/WSGI hot-path work, batch-first dispatch, native routing, TFB/PGO, and explicitly cancels the IPC-heavy direction.
- The most important historical lesson is that `litecore` repeatedly moved away from "every request goes through worker IPC" and eventually also moved away from "HTTP in C++, tooling in Rust." The surviving direction is "Rust owns the HTTP hot path, PyO3 is the bridge, and native routing/batching/codegen do more of the work."

## Codebase Analysis

### Current architecture in `feat/mvp-v3-post`

The current branch is no longer organized around the old v1 shared-memory worker substrate or the v2 C++ server.

The local tree shows:

- a Rust workspace with `litecore-bundler`, `litecore-http`, `litecore-js`, and `litecore-storage`
- no `src/cpp/`
- no Python CFFI entrypoint like `src/py/litecore/_native.py`

The current architecture document describes a Python supervisor spawning worker processes that host a Rust HTTP core bridged through PyO3, with lazy request materialization and free-threaded Python readiness:

- `/home/cody/code/c/litecore/docs/mintlify/pages/internals/architecture.mdx`

The current README also treats the CFFI period as historical:

- `/home/cody/code/c/litecore/README.md`

Relevant line of evidence from the current README:

- "Legacy CFFI benchmarks are retained for historical context."

That matches the tree state. The C++/CFFI runtime is gone from the active branch.

### What the current tree still carries forward from the earlier experiments

The branch pivots were real, but several ideas survived across them:

- direct native HTTP ownership rather than Python socket handling
- lazy request/response materialization to reduce Python overhead
- Rust-backed bundler/tooling instead of shelling out blindly to JS tools
- type generation and virtual-module workflows as first-class features
- aggressive benchmarking, profiling, and perf gates as part of normal development

The native bundler path is now visible in:

- `/home/cody/code/c/litecore/src/py/litecore/framework/bundler/native.py`

That file shows the present direction clearly: Python orchestrates config, then hands build/dev operations to a Rust-backed `RolldownBundler`.

## Library Documentation

No external library documentation was required for this pass because the question was about local repository history, not current upstream APIs.

The authoritative sources for this research were:

- local git history from `/home/cody/code/c/litecore`
- current architecture docs in `/home/cody/code/c/litecore/docs/mintlify/pages/internals/architecture.mdx`
- current README notes in `/home/cody/code/c/litecore/README.md`
- branch-local project patterns captured in `feat/mvp-3` via `.agent/patterns.md`

The key branch-local documentation signal is that `feat/mvp-3` explicitly labels the CFFI/uWebSockets stack as archived historical guidance, not current practice.

## Prior Art

### Branch timeline

#### `main`

`main` is mostly bootstrap context. It does not represent the runtime architecture that `litecore` later explored.

#### `feat/mvp` through February 1, 2026

Head: `39a0c74` on February 1, 2026

This was the original "native Rust server plus protocol bridge" era.

What it added:

- PyO3-backed Rust server startup and Tokio runtime ownership
- RSGI request/response objects and dispatch
- ASGI scope builder, send/receive channels, lifespan, body streaming
- WSGI bridge support
- benchmark harness and comparison targets
- a full zero-copy shared-memory subsystem:
  - shared memory layer
  - synchronization primitives
  - buffer and slot layout
  - lock-free ring buffers
  - worker process management
  - HTTP-to-shared-memory bridge
  - Python `_shm`, `_worker`, and `_asgi` support
  - slab allocator and duplex streaming follow-on work

Interpretation:

- This branch proves `litecore` seriously tried the same broad shared-memory-worker architecture that later work in `vertebra` is reconsidering.
- It was not a tiny side experiment. It was a large first-generation design.
- The later branch history matters because the project moved away from making this the center of the product.

#### `feat/mvp-2` on February 1-2, 2026

Head: `2ce55c4` on February 2, 2026

This was the most dramatic reboot.

Key commits:

- `e3a5734` on February 1, 2026:
  - archives the v1 IPC code
  - removes the entire old Rust workspace from source
  - rewrites project context for a v2 CFFI rewrite
- `b9740e4` on February 1, 2026:
  - adds `src/cpp/`
  - adds uWebSockets/uSockets submodules
  - implements a C API in `litecore.h`
  - implements a C++ wrapper in `litecore.cpp`
- `ee08226` on February 1, 2026:
  - adds Python CFFI bindings
  - adds `_native.py`, `_protocol.py`, `_asgi.py`, and `server.py`

What else this branch added:

- a Type Bridge system
- a native Rolldown bundler with PyO3 bindings
- test/build infrastructure
- protocol expansion work for TLS, WebSocket/SSE, HTTP/2 API stubs, HTTP/3 infrastructure
- wheel/build packaging for native libraries
- object pooling and hot-path tuning
- Docker benchmark infrastructure
- H2C proxy integration for Cloud Run

Interpretation:

- Your memory is correct: `litecore` absolutely did try a C++ route.
- More precisely, it tried a dual-native architecture:
  - C++ for the HTTP server
  - Rust/PyO3 for the bundler
- This was not the end state. It was a transitional reboot after dissatisfaction with the v1 IPC-heavy architecture.

#### `feat/mvp-3` on February 3, 2026

Head: `bdd4014` on February 3, 2026

This branch is the explicit "walk away from CFFI" branch.

Key signal commit:

- `bdd4014` on February 3, 2026:
  - rewrites docs/specs around a Rust-first HTTP core migration
  - removes legacy CFFI guidance from project patterns
  - archives the CFFI-era plan
  - removes the need for the earlier h2c proxy layer

What this branch focused on:

- bridge-tax elimination through batched dispatch
- codegen and extractor caching
- digest caches and IR/type generation
- file watching and websocket HMR
- virtual module support
- production type output
- benchmark validation gates

Interpretation:

- `feat/mvp-3` is the clearest evidence that the C++ experiment was judged temporary.
- Architecturally, the project had already decided that the better path was not "stick with CFFI and optimize harder" but "return to a Rust-owned hot path and reduce bridge cost there."

#### `feat/mv-v3-pre-asgi` on February 4, 2026

Head: `ec56df4` on February 4, 2026

This branch is mostly DX and native bundler groundwork.

What it added:

- ESM dependencies and TypeScript config
- native ESM resolution with Rolldown integration
- template restructuring for native bundler mode
- dev-server wiring for the native bundler
- examples using the native mode layout
- CI/CD for Python and NPM publishing

Interpretation:

- This is not the branch that defines the runtime model.
- It is the branch that makes the tooling/dev story coherent before the next runtime expansion.

#### `feat/mvp-v3-post` from February 4 to February 26, 2026

Head: `c339b40` on February 26, 2026

This is the current and most important branch.

What it added:

- framework package reorganization
- route decorators, controllers, DI, middleware, guards, events/signals
- ASGI runtime redesign and hot-path performance work
- WSGI redesign
- Rust asset server and dev proxy
- meta-framework adapters and frontend integration
- Python 3.14 free-threading groundwork
- AI-serving features like SSE, batching, and backpressure controls
- MCP and gRPC support
- embedded JS runtime work
- plugin-first framework composition
- Type Bridge v2 emitters and incremental invalidation
- batch-first framework redesign
- embedded hosting and ASGI/WSGI mount forwarding
- broker-backed DB transport experiments
- PGO pipeline
- performance-recovery work
- Rust-native access logging
- Rust-backed storage
- native routing and route-id handoff
- Techempower benchmark infrastructure and verification

The two most important architectural signals in this branch are:

- `d86f3de` on February 9, 2026:
  - "batch-first redesign complete — Litecore is no longer ASGI"
- `b996d71` on February 20, 2026:
  - archives `litecore-6wm` as superseded
  - explicitly says `litecore-ipc` is cancelled

Interpretation:

- The current branch did not converge on shared-memory IPC as the main system boundary.
- It converged on native batch dispatch, native routing, direct Rust/PyO3 integration, and selective transport work where it still made sense.

### What survived and what died

#### Survived into the current line

- Rust ownership of the HTTP hot path
- PyO3 as the main Python/native bridge
- lazy request materialization
- benchmark-first optimization culture
- native bundler and Type Bridge work
- routing and dispatch in Rust
- batch-first execution as an organizing principle

#### Abandoned or demoted

- v1 shared-memory IPC as the universal execution model
- the C++/uWebSockets HTTP core
- Python CFFI as the primary native bridge
- the h2c proxy as a long-term architectural layer
- older compatibility and fallback asset-pipeline paths

## Risk Assessment

- Historical docs in `litecore` can mislead if read without branch context. Some archived PRDs describe architectures that were later explicitly cancelled.
- The project pivoted very quickly between February 1 and February 3, 2026. A superficial read of only one intermediate branch can produce the wrong conclusion about the intended direction.
- Benchmark claims from the CFFI or early IPC eras should not be treated as directly comparable to later Rust-first or batch-first branches.
- `feat/mvp-v3-post` is broad and ambitious. It is the best signal for direction, but not every subsystem in it is equally mature.

## Recommended Approach

- If you want the "what did we actually learn?" answer, treat `feat/mvp-v3-post` as the current truth, `feat/mvp-3` as the explicit rationale for the Rust-first pivot, `feat/mvp-2` as the historical C++ experiment, and `feat/mvp` as the record of the first shared-memory-worker architecture.
- If you are using `litecore` as prior art for `vertebra`, do not over-index on the v1 IPC work. The repo’s own history shows that the project kept moving away from that as the default answer for general HTTP serving.
- If you are deciding whether to revive the C++ approach, the history argues against it. `litecore` tried it, then abandoned it almost immediately in favor of Rust-first ownership.
- The strongest transferable lessons are:
  - reduce bridge tax before adding more cross-process complexity
  - keep routing and serialization on the native side
  - use IPC selectively where the data plane really benefits
  - treat profiling, TFB, and perf gates as architectural feedback, not post-hoc validation
