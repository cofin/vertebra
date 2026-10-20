---
okf_version: "0.2"
title: "Vertebra OKF Knowledge Bundle"
description: "Root index for Vertebra's Open Knowledge Format (OKF v0.2) product definition, nested engineering knowledge base, and bundle activity log."
status: stable
tags:
  - okf
  - index
  - vertebra
---

# Vertebra OKF Bundle (`v0.2`)

Root index for **Vertebra** — the Rust-powered, full-stack Python & TypeScript application backbone (`vertebra` + `@vertebra/client`).

---

## 1. Product Definition (`product/`)

- **[Product Overview](product/product.md)** — Vision, core capabilities, and runtime topologies (`Embedded` + `SharedMemory`, Python `3.12`–`3.14t`).
- **[Product & Engineering Guidelines](product/product-guidelines.md)** — Clean-break domain naming rules, zero-copy hot-path invariants, Python module layout, and Git/PR discipline.
- **[Technology Stack](product/tech-stack.md)** — Complete Rust (`vertebra-core`, `vertebra-http`, `vertebra-py`, `vertebra-bundler`), Python (`vertebra`), and TypeScript (`@vertebra/client`) dependency and toolchain matrix.

## 2. Engineering Knowledge Base (`knowledge/`)

- **[Knowledge Base Index](knowledge/index.md)** — Master progressive-disclosure index for all nested knowledge chapters.
- **[Project Workflow & Commands](knowledge/workflow.md)** — Canonical `Makefile` (`make setup-env`, `make install`, `make dev`, `make test`, `make lint-all`, `make check`, `make bench`, `make bench-ft`), polyglot rebuild cycle, and Flow OKF v0.2 execution rules.
- **[Architecture (`knowledge/architecture/`)](knowledge/architecture/index.md)**:
  - `runtime/`: [Topologies & Python Embedding](knowledge/architecture/runtime/topologies-and-embedding.md), [SHM Supervision & Crash Recovery](knowledge/architecture/runtime/shm-supervision-and-recovery.md)
  - `ipc/`: [Lock-Free Ring Buffers & SHM](knowledge/architecture/ipc/ring-buffers-and-shm.md), [Tiered Rings & Batch Bridge](knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md)
  - `http/`: [HTTP/1.1 & HTTP/2 Ingress & Routing](knowledge/architecture/http/ingress-and-routing.md), [Request Batching, Dispatch & Backpressure](knowledge/architecture/http/batching-dispatch-and-backpressure.md)
  - `worker/`: [PyO3 Zero-Copy FFI & Batch Dispatch](knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md), [Task-Free ASGI & Sync WSGI Runners](knowledge/architecture/worker/asgi-and-wsgi-runners.md)
  - `frontend/`: [Oxc Bundler, HMR & QuickJS SSR Pool](knowledge/architecture/frontend/bundler-hmr-and-ssr.md), [TypeScript/Zod Codegen & Inertia v3](knowledge/architecture/frontend/codegen-and-inertia.md)
- **[Domains (`knowledge/domains/`)](knowledge/domains/index.md)**:
  - [Python Framework, Routing & Serialization (`framework/`)](knowledge/domains/framework/routing-and-app.md)
  - [Python Server, Worker Loop & Protocol Adapters (`server/`)](knowledge/domains/server/worker-and-protocols.md)
  - [Rust IPC, Shared Memory & Ring Transport (`ipc/`)](knowledge/domains/ipc/transport-and-rings.md)
  - [Rust HTTP Ingress, Batching & Dispatch (`http/`)](knowledge/domains/http/server-and-dispatch.md)
  - [Frontend Bundler, Codegen & TypeScript SDK (`frontend/`)](knowledge/domains/frontend/bundler-and-client-sdk.md)
- **[Patterns (`knowledge/patterns/`)](knowledge/patterns/index.md)**:
  - `python/`: [Module & Class Layout](knowledge/patterns/python/module-and-class-layout.md), [Hot-Path Caching & Single-Wake Async Dispatch](knowledge/patterns/python/hot-path-and-async-dispatch.md)
  - `rust/`: [PyO3 0.30.1 FFI & GIL Discipline](knowledge/patterns/rust/ffi-and-gil-discipline.md), [Lock-Free SHM & Sharded Concurrency](knowledge/patterns/rust/lock-free-shm-and-sharding.md)
  - `typescript/`: [Client SDK & Framework Adapter Subpaths](knowledge/patterns/typescript/client-and-adapter-subpaths.md)
- **[Testing (`knowledge/testing/`)](knowledge/testing/index.md)**:
  - [Polyglot Test Matrix & Environment Setup](knowledge/testing/polyglot-test-matrix.md), [Benchmark Suite & Free-Threaded Python 3.14t](knowledge/testing/benchmarks-and-free-threaded.md)
- **[Gotchas (`knowledge/gotchas/`)](knowledge/gotchas/index.md)**:
  - [PyO3 FFI, Embedded Globals & Python Runtime Gotchas](knowledge/gotchas/pyo3-and-python-runtime.md), [Shared-Memory Rings & Cross-Platform IPC Gotchas](knowledge/gotchas/shm-and-cross-platform-ipc.md)

## 3. Specifications, Research & Activity Log

- **`specs/` (lazy)** — Materialized only when planned or active flows exist; completed flows are contracted into `knowledge/` and `log.md`.
- **`research/` (lazy)** — Materialized only when un-promoted technical research briefs exist.
- **[Bundle Activity Log](log.md)** — Chronological log of bundle initialization, syntheses, and archived flows.
