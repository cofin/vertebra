---
type: Guide
title: Vertebra Product Definition
description: Product mission, polyglot runtime architecture, and developer experience capabilities of the Vertebra application backbone.
status: stable
tags: [product, runtime, python, rust, typescript]
sources:
  - resource: README.md
  - resource: pyproject.toml
  - resource: src/py/vertebra/__init__.py
  - resource: src/rs/Cargo.toml
  - resource: src/ts/vertebra-client/package.json
---

# Vertebra Product Definition

<!-- truth: start -->
## Project Purpose

Vertebra is a Python-first, ultra-low-latency application backbone designed to deliver native Rust throughput (`123,790 req/s` direct dispatch, `113,030 req/s` bridge dispatch, `<7 ms` in-process Python import) and a cohesive full-stack developer experience across Linux, macOS, Windows, containers, and cloud runtimes.

## Core Product Capabilities

- **Python-First Ergonomics (`vertebra`)**: Declarative [`App`](../../../src/py/vertebra/framework/_app.py) and [`Router`](../../../src/py/vertebra/framework/_router.py) routing with typed path parameters, sub-application mounting (`App.mount`), lifecycle hooks (`on_startup`, `on_shutdown`), Inertia.js v3 protocol middleware ([`InertiaMiddleware`](../../../src/py/vertebra/framework/_inertia.py)), SSR orchestration, and end-to-end Python-to-TypeScript/Zod code generation ([`BuildOrchestrator`](../../../src/py/vertebra/builder/_orchestrator.py), [`TypeExtractor`](../../../src/py/vertebra/builder/_codegen/_extractor.py)).
- **Multi-Protocol Execution**: First-class execution for native Vertebra handlers (`RequestBatch`, `Request`, `Response`, `Headers`), ASGI 3.0 applications ([`AsgiRunner`](../../../src/py/vertebra/server/_asgi/_runner.py) with task-free `_drive_coro` and [`AsgiScope`](../../../src/rs/vertebra-py/src/asgi_scope.rs)), and PEP 3333 WSGI applications ([`WsgiAdapter`](../../../src/py/vertebra/server/_wsgi/_adapter.py) and [`WsgiEnviron`](../../../src/rs/vertebra-py/src/wsgi_environ.rs)).
- **Dual Runtime Topologies**:
  - **Embedded (`--topology embedded`)**: Single-process execution pairing the Tokio/Hyper HTTP server and Python worker threads in one address space via PyO3 (`append_to_inittab!(_vertebra)`) and [`BatchBridge`](../../../src/rs/vertebra-core/src/bridge.rs), with first-class support for free-threaded Python (`3.14t`, `FREE_THREADED = true`).
  - **Shared Memory (`--topology shm`)**: Multi-process scale-out and fault isolation connecting the Rust HTTP ingress to independent Python worker processes over lock-free [`MpmcRing`](../../../src/rs/vertebra-core/src/ipc.rs) request rings and 4-tier [`TieredRingSet`](../../../src/rs/vertebra-core/src/tiered_ring.rs) (`SpscRing`) response rings with `eventfd`/`ulock`/pipe wakeups.
- **Native HTTP, Bundling & SSR (`vertebra-http`, `vertebra-bundler`)**: Hyper HTTP/1.1 (`pipeline_flush`) and HTTP/2 server, `rustls`/`rcgen` TLS, zstd/gzip/brotli compression, admission-control [`BackpressureController`](../../../src/rs/vertebra-http/src/backpressure.rs) with sliding-window circuit breaking, embedded Oxc JS/TS transforms ([`Bundler`](../../../src/rs/vertebra-bundler/src/bundler.rs)), QuickJS SSR context pooling ([`SsrPool`](../../../src/rs/vertebra-bundler/src/ssr.rs)), and HMR WebSockets.
- **Full-Stack TypeScript Client (`@vertebra/client`)**: Zero-dependency [`HttpClient`](../../../src/ts/vertebra-client/src/client/client.ts) with retry/backoff, [`RealtimeChannel`](../../../src/ts/vertebra-client/src/client/ws.ts) WebSockets, [`SessionStore`](../../../src/ts/vertebra-client/src/store/index.ts) reactive state, [`HotReloadClient`](../../../src/ts/vertebra-client/src/hmr/index.ts), and subpath adapters for React 19, Vue 3.5, Svelte 5, Angular 22, HTMX, and Vite 8.
<!-- truth: end -->

## Architectural Overview

| Layer | Package / Crate | Primary Responsibility |
| --- | --- | --- |
| Python Framework & Server | [`src/py/vertebra/`](../../../src/py/vertebra/__init__.py) | `App`, `Router`, `InertiaMiddleware`, `BuildOrchestrator`, `AsgiRunner`, `WsgiAdapter`, `AsyncRuntime`, `BatchDispatchPlan`, `vtb` CLI, and `WorkerSupervisor` |
| Rust Core IPC & Wire Protocol | [`src/rs/vertebra-core/`](../../../src/rs/vertebra-core/src/lib.rs) | `ShmRegion`, lock-free `SpscRing` (`SlotView`) and `MpmcRing`, `BatchBridge`, `RingPair`, `TieredRingSet`, `CompressionCodec` (LZ4/Zstd), and OS sync (`eventfd`, `ulock`, pipes) |
| Rust HTTP Ingress & Dispatch | [`src/rs/vertebra-http/`](../../../src/rs/vertebra-http/src/lib.rs) | Tokio/Hyper HTTP/1.1 & HTTP/2 accept loop, `Router` (`matchit 0.9`), `RouteTable`, 8-shard `RequestBatcher` (`BatchPolicy`), `Dispatcher`, 256-shard `InFlightRequests`, `BackpressureController`, and `RingFactory` |
| Rust Python FFI Extension | [`src/rs/vertebra-py/`](../../../src/rs/vertebra-py/src/lib.rs) | PyO3 `vertebra._vertebra` slab-backed classes (`Request`, `Response`, `Headers`, `RequestBatch`, `AsgiScope`, `WsgiEnviron`) and vectorized batch API (`init_transport`, `get_batch`, `try_get_batch`, `submit_response`, `submit_results`) |
| Rust Native Bundler & SSR | [`src/rs/vertebra-bundler/`](../../../src/rs/vertebra-bundler/src/lib.rs) | PyO3 `vertebra._bundler` module backed by `oxc` (`0.155`) for TS/JSX transforms, `rolldown-notify` `SourceWatcher` for HMR, and `rquickjs` (`0.14`) `SsrPool` |
| TypeScript Client SDK | [`src/ts/vertebra-client/`](../../../src/ts/vertebra-client/src/index.ts) | `@vertebra/client` multi-entry package (`HttpClient`, `RealtimeChannel`, `SessionStore`, `HotReloadClient`, and React/Vue/Svelte/Angular/HTMX/Vite bindings) |

## Related Context

- [Product Guidelines](product-guidelines.md)
- [Tech Stack](tech-stack.md)
- [Knowledge Index](../knowledge/index.md)
