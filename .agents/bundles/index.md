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

- **[Product Overview](product/product.md)** — Vision, core capabilities, runtime topologies (`Embedded` + `Shm`, Python `3.12`–`3.14t`), multicloud object storage, database drivers, telemetry, and Litestar/SQLSpec/Alchemy/APEX/gRPC integrations.
- **[Product & Engineering Guidelines](product/product-guidelines.md)** — Clean-break domain naming rules, zero-copy hot-path invariants, `msgspec.Struct(..., frozen=True, gc=False)` value objects, Python module layout, and Git/PR discipline.
- **[Technology Stack](product/tech-stack.md)** — Complete Rust workspace (`vertebra-core`, `vertebra-http`, `vertebra-py`, `vertebra-bundler`, `vertebra-storage`), Python (`vertebra`), and TypeScript (`@vertebra/client`) dependency and toolchain matrix.

## 2. Engineering Knowledge Base (`knowledge/`)

- **[Knowledge Base Index](knowledge/index.md)** — Master progressive-disclosure index for all nested knowledge chapters.
- **[Project Workflow & Commands](knowledge/workflow.md)** — Canonical `Makefile` (`make setup-env`, `make install`, `make dev`, `make test`, `make lint-all`, `make review-check`, `make check`, `make build`, `make bench`, `make bench-ft`, `make bench-baseline`, `make bench-check`, `make upgrade`), polyglot rebuild cycle, and Flow OKF v0.2 execution rules.
- **[Architecture (`knowledge/architecture/`)](knowledge/architecture/index.md)**:
  - `runtime/`: [Runtime Topologies & Python Embedding](knowledge/architecture/runtime/topologies-and-embedding.md), [SHM Worker Spawn, Shutdown & Recovery Hooks](knowledge/architecture/runtime/shm-supervision-and-recovery.md), [Server Platform & Process Lifecycle](knowledge/architecture/runtime/platform-and-lifecycle.md)
  - `ipc/`: [Lock-Free Ring Buffers & Shared Memory](knowledge/architecture/ipc/ring-buffers-and-shm.md), [Tiered Rings & In-Process Batch Bridge](knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md)
  - `http/`: [HTTP Ingress, Request Pipeline & Native Routing](knowledge/architecture/http/ingress-and-routing.md), [Request Batching, Dispatch & Backpressure](knowledge/architecture/http/batching-dispatch-and-backpressure.md), [Protocol and Offload Modules in vertebra-http](knowledge/architecture/http/protocol-and-offload-modules.md)
  - `worker/`: [PyO3 FFI & Vectorized Batch Dispatch](knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md), [ASGI, RSGI & WSGI Runners](knowledge/architecture/worker/asgi-and-wsgi-runners.md)
  - `frontend/`: [Oxc Bundler, HMR Watcher & QuickJS SSR Pool](knowledge/architecture/frontend/bundler-hmr-and-ssr.md), [Route Codegen Targets & Inertia v3 Middleware](knowledge/architecture/frontend/codegen-and-inertia.md)
- **[Domains (`knowledge/domains/`)](knowledge/domains/index.md)**:
  - `framework/`: [Python Framework, Routing & Serialization Domain](knowledge/domains/framework/routing-and-app.md), [Framework Subdomains](knowledge/domains/framework/subdomains.md), [Distributed Queues, Cron Leader Election & Transactional Email](knowledge/domains/framework/distributed-queues-and-email.md), [Live State Synchronization](knowledge/domains/framework/live-state.md), [`vtb` Command-Line Interface](knowledge/domains/framework/cli.md)
  - `agent/`: [Agentic Harness, MCP 2026-07-28 & A2A 1.0 Protocols](knowledge/domains/agent/mcp-and-a2a-protocol-harness.md)
  - `server/`: [Python Server, Worker Loop & Protocol Adapters Domain](knowledge/domains/server/worker-and-protocols.md), [Multi-Lane Worker Runner](knowledge/domains/server/worker-lanes.md)
  - `ipc/` & `http/`: [Rust IPC, Shared Memory & Ring Transport Domain](knowledge/domains/ipc/transport-and-rings.md), [Rust HTTP Ingress, Batching & Dispatch Domain](knowledge/domains/http/server-and-dispatch.md)
  - `frontend/`: [Frontend Bundler, Codegen & Client SDK Domain](knowledge/domains/frontend/bundler-and-client-sdk.md), [Standalone & Onefile Packaging and PGO Tooling](knowledge/domains/frontend/packaging-and-pgo.md), [WASM Plugin Host & Browser Bridge](knowledge/domains/frontend/wasm.md)
  - `storage/`, `data/` & `telemetry/`: [Object Storage, Presigning & Upload Tracking](knowledge/domains/storage/object-storage.md), [Shared-Memory Connection Broker & Statement Cache](knowledge/domains/data/broker-and-shm-leases.md), [Database Drivers & Cloud Dialers](knowledge/domains/data/database-drivers-and-dialers.md), [Query Pipelines, Change Feeds, Tenant Predicates & DataFusion](knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [Logging, Trace Context, SQL Trace Propagation & GenAI Metrics](knowledge/domains/telemetry/logging-tracing-and-genai.md)
  - `extensions/`: [Litestar Integration: Route Compiler, Middleware Offload, and Fast Path](knowledge/domains/extensions/litestar.md), [SQLSpec, Advanced Alchemy & SQLAlchemy Dialect Extensions](knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md), [Oracle APEX, ORDS & 26ai Duality View Integration](knowledge/domains/extensions/oracle-apex.md), [gRPC Services & Wire Framing](knowledge/domains/extensions/grpc.md)
- **[Patterns (`knowledge/patterns/`)](knowledge/patterns/index.md)**:
  - `python/`: [Python Module Structure, Class Layout & PEP 695 Typing](knowledge/patterns/python/module-and-class-layout.md), [Hot-Path Caching & Single-Wake Async Dispatch](knowledge/patterns/python/hot-path-and-async-dispatch.md), [Serialization & Python Utility Conventions](knowledge/patterns/python/serialization-and-utils.md)
  - `rust/`: [PyO3 0.30.1 FFI & GIL Discipline](knowledge/patterns/rust/ffi-and-gil-discipline.md), [Lock-Free Shared Memory & Sharded Concurrency](knowledge/patterns/rust/lock-free-shm-and-sharding.md)
  - `typescript/`: [Client SDK Root Exports, Subpaths & Build Entries](knowledge/patterns/typescript/client-and-adapter-subpaths.md)
- **[Testing (`knowledge/testing/`)](knowledge/testing/index.md)**:
  - [Polyglot Test Matrix & Environment Setup](knowledge/testing/polyglot-test-matrix.md), [Benchmark Suite & Free-Threaded Python 3.14t](knowledge/testing/benchmarks-and-free-threaded.md), [Agentic Review & Benchmark Gates](knowledge/testing/agentic-review.md)
- **[Gotchas (`knowledge/gotchas/`)](knowledge/gotchas/index.md)**:
  - [PyO3 FFI, Embedded Globals & Python Runtime Gotchas](knowledge/gotchas/pyo3-and-python-runtime.md), [Shared-Memory Rings & Cross-Platform IPC Gotchas](knowledge/gotchas/shm-and-cross-platform-ipc.md)

## 3. Specifications, Research & Activity Log

- **`specs/` (lazy)** — Materialized only when active or planned Flow specifications exist.
- **`research/` (lazy)** — Materialized only when un-promoted technical research briefs exist.
- **[Bundle Activity Log](log.md)** — Chronological log of bundle initialization, syntheses, and archived flows.
