---
okf_version: "0.2"
bundle: "vertebra"
title: "Vertebra OKF Knowledge Bundle"
description: "Root index for Vertebra's Open Knowledge Format (OKF v0.2) product definition, nested engineering knowledge base, and bundle activity log."
tags:
  - okf
  - index
  - vertebra
---

# Vertebra OKF Bundle (`v0.2`)

Root index for **Vertebra** — the Rust-powered, full-stack Python & TypeScript application backbone (`vertebra` + `@vertebra/client`).

## 1. Product Definition ([`product/`](product/index.md))

- **[Product Overview](product/product.md)** — Vision, core capabilities, runtime topologies (`Auto`, `Embedded`, and `Shm`, Python `3.12`–`3.15` and free-threaded `3.14t`/`3.15t`), multicloud object storage, database drivers, telemetry, and Litestar/Starlette/FastAPI/Flask/Django/SQLSpec/Alchemy/APEX/gRPC integrations.
- **[Product & Engineering Guidelines](product/product-guidelines.md)** — Clean-break domain naming rules, deep-module seam classification, zero-copy hot-path invariants, `msgspec.Struct(..., frozen=True, gc=False)` value objects, Python module layout, and Git/PR discipline.
- **[Technology Stack](product/tech-stack.md)** — Complete Rust workspace (`vertebra-core`, `vertebra-http`, `vertebra-py`, `vertebra-bundler`, `vertebra-storage`, `vertebra-db`, `vertebra-bigquery`, `vertebra-spanner`, `vertebra-duckdb`), Python (`vertebra-core` PyPI distribution / `vertebra` import namespace), and TypeScript (`@vertebra/client` `0.3.0`) dependency and toolchain matrix.

## 2. Engineering Knowledge Base (`knowledge/`)

- **[Knowledge Base Index](knowledge/index.md)** — Master progressive-disclosure index for all nested knowledge chapters.
- **[Project Workflow & Commands](knowledge/workflow.md)** — Canonical `Makefile` (`make setup-env`, `make install`, `make dev`, `make test`, `make lint-all`, `make review-check`, `make check`, `make build`, `make build-pgo`, `make bench`, `make bench-ft`, `make bench-baseline`, `make bench-check`, `make upgrade`), polyglot rebuild cycle, and Flow OKF v0.2 execution rules.
- **[Architecture (`knowledge/architecture/`)](knowledge/architecture/index.md)**:
  - `runtime/`: [Runtime Topologies & Python Embedding](knowledge/architecture/runtime/topologies-and-embedding.md), [SHM Worker Spawn, Shutdown & Recovery Hooks](knowledge/architecture/runtime/shm-supervision-and-recovery.md), [Server Platform & Process Lifecycle](knowledge/architecture/runtime/platform-and-lifecycle.md)
  - `ipc/`: [Lock-Free Ring Buffers & Shared Memory](knowledge/architecture/ipc/ring-buffers-and-shm.md), [Tiered Rings & In-Process Batch Bridge](knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md)
  - `http/`: [HTTP Ingress, Request Pipeline & Native Routing](knowledge/architecture/http/ingress-and-routing.md), [Request Batching, Dispatch & Backpressure](knowledge/architecture/http/batching-dispatch-and-backpressure.md), [Protocol and Offload Modules in vertebra-http](knowledge/architecture/http/protocol-and-offload-modules.md)
  - `worker/`: [PyO3 FFI & Vectorized Batch Dispatch](knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md), [ASGI, RSGI & WSGI Runners](knowledge/architecture/worker/asgi-and-wsgi-runners.md)
  - `frontend/`: [Oxc Bundler, HMR Watcher & QuickJS SSR Pool](knowledge/architecture/frontend/bundler-hmr-and-ssr.md), [Route Codegen Targets & Inertia v3 Middleware](knowledge/architecture/frontend/codegen-and-inertia.md)
- **[Domains (`knowledge/domains/`)](knowledge/domains/index.md)**:
  - `framework/`: [Python Framework, Routing & Serialization Domain](knowledge/domains/framework/routing-and-app.md), [Framework Subdomains](knowledge/domains/framework/subdomains.md), [Distributed Queues, Cron Leader Election & Transactional Email](knowledge/domains/framework/distributed-queues-and-email.md), [Live State Synchronization](knowledge/domains/framework/live-state.md), [`vtb` Command-Line Interface](knowledge/domains/framework/cli.md)
  - `agent/`: [Agentic Harness, MCP 2026-07-28 & A2A 1.0 Protocols](knowledge/domains/agent/mcp-and-a2a-protocol-harness.md), [Shared-Memory Vector Slabs, Zero-Weight Cloud & SQL Embeddings, and Local Broker](knowledge/domains/agent/embeddings-and-vectors.md)
  - `server/`: [Python Server, Worker Loop & Protocol Adapters Domain](knowledge/domains/server/worker-and-protocols.md), [Multi-Lane Worker Runner](knowledge/domains/server/worker-lanes.md)
  - `ipc/` & `http/`: [Rust IPC, Shared Memory & Ring Transport Domain](knowledge/domains/ipc/transport-and-rings.md), [Rust HTTP Ingress, Batching & Dispatch Domain](knowledge/domains/http/server-and-dispatch.md)
  - `frontend/`: [Frontend Bundler, Codegen & Client SDK Domain](knowledge/domains/frontend/bundler-and-client-sdk.md), [Standalone & Onefile Packaging and PGO Tooling](knowledge/domains/frontend/packaging-and-pgo.md), [WASM Plugin Host & Browser Bridge](knowledge/domains/frontend/wasm.md)
  - `storage/`, `data/` & `telemetry/`: [Object Storage, Streaming & Presigning](knowledge/domains/storage/object-storage.md), [Object Storage Implementation Limitations](knowledge/domains/storage/object-storage-limitations.md), [Shared-Memory Connection Broker & Statement Cache](knowledge/domains/data/broker-and-shm-leases.md), [Database Drivers & Cloud Dialers](knowledge/domains/data/database-drivers-and-dialers.md), [GCP BigQuery & Spanner Native Drivers](knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md), [Query Pipelines, Change Feeds, Tenant Predicates & DataFusion](knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [Logging, Trace Context, SQL Trace Propagation & GenAI Metrics](knowledge/domains/telemetry/logging-tracing-and-genai.md)
  - `extensions/`: [Framework Extensions: Litestar, Starlette, FastAPI, Flask, and Django](knowledge/domains/extensions/litestar.md), [SQLSpec, Advanced Alchemy & SQLAlchemy Dialect Extensions](knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md), [Oracle APEX, ORDS & 26ai Duality View Integration](knowledge/domains/extensions/oracle-apex.md), [gRPC Services & Wire Framing](knowledge/domains/extensions/grpc.md)
- **[Patterns (`knowledge/patterns/`)](knowledge/patterns/index.md)**:
  - `python/`: [Python Module Structure, Class Layout & PEP 695 Typing](knowledge/patterns/python/module-and-class-layout.md), [Hot-Path Caching & Single-Wake Async Dispatch](knowledge/patterns/python/hot-path-and-async-dispatch.md), [Serialization & Python Utility Conventions](knowledge/patterns/python/serialization-and-utils.md)
  - `rust/`: [PyO3 0.30.1 FFI & GIL Discipline](knowledge/patterns/rust/ffi-and-gil-discipline.md), [Lock-Free Shared Memory & Sharded Concurrency](knowledge/patterns/rust/lock-free-shm-and-sharding.md)
  - `typescript/`: [Client SDK Root Exports, Subpaths & Build Entries](knowledge/patterns/typescript/client-and-adapter-subpaths.md)
- **[Testing (`knowledge/testing/`)](knowledge/testing/index.md)**:
  - [Polyglot Test Matrix & Environment Setup](knowledge/testing/polyglot-test-matrix.md), [Benchmark Suite & Free-Threaded Python 3.14t/3.15t](knowledge/testing/benchmarks-and-free-threaded.md), [Agentic Review & Benchmark Gates](knowledge/testing/agentic-review.md)
- **[Gotchas (`knowledge/gotchas/`)](knowledge/gotchas/index.md)**:
  - [PyO3 FFI, Embedded Globals & Python Runtime Gotchas](knowledge/gotchas/pyo3-and-python-runtime.md), [Shared-Memory Rings & Cross-Platform IPC Gotchas](knowledge/gotchas/shm-and-cross-platform-ipc.md)
- **[Decisions (`knowledge/decisions/`)](knowledge/decisions/index.md)**:
  - [ADR-001: Dual-Topology Embedded and Shared-Memory Runtime](knowledge/decisions/adr-001-dual-topology-embedded-and-shm-runtime.md), [ADR-002: Shared-Memory Database Broker and Arrow IPC Data Plane](knowledge/decisions/adr-002-two-tier-shm-database-and-arrow-data-plane.md), [ADR-003: PyPI `vertebra-core` Distribution and Wheel-Bundled TypeScript Client](knowledge/decisions/adr-003-pypi-vertebra-core-distribution-and-wheel-bundled-client.md), [ADR-004: Optional Driver Wheels and SHM Capsule Ownership](knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md), [ADR-005: Persistent Object-Store TTL Envelopes](knowledge/decisions/adr-005-persistent-object-store-ttl-envelopes.md)

## 3. Specifications, Research & Activity Log

- **[`specs/`](specs/index.md) (active)** —
  - **Master PRDs (`state: planned`)**: [`native-data-platform`](specs/native-data-platform/spec.md) and [`integration-remediation`](specs/integration-remediation/spec.md)
  - **Planned Child Flows (`state: planned`)**: [`native-postgres-driver`](specs/native-postgres-driver/spec.md) (`t01`–`t102`) delivers the standalone Rust PostgreSQL wheel (`vertebra-postgres`).
  - **Draft Child Flows (`state: draft`)**: [`broker-pool-sharing`](specs/broker-pool-sharing/spec.md), [`native-oracle-driver`](specs/native-oracle-driver/spec.md), [`native-mssql-driver`](specs/native-mssql-driver/spec.md), [`native-sqlite-driver`](specs/native-sqlite-driver/spec.md), [`native-redis-driver`](specs/native-redis-driver/spec.md), [`sqlalchemy-and-django-integration`](specs/sqlalchemy-and-django-integration/spec.md), [`sqlspec-adapter-parity`](specs/sqlspec-adapter-parity/spec.md), [`queues-tasks-and-channels`](specs/queues-tasks-and-channels/spec.md)
  - **Active Research**: [`gap-analysis_20261103.md`](specs/native-data-platform/research/gap-analysis_20261103.md), [`rust-driver-crates_20261103.md`](specs/native-data-platform/research/rust-driver-crates_20261103.md), [`protocol-and-execution_20261105.md`](specs/native-postgres-driver/research/protocol-and-execution_20261105.md), [`cloud-material_20261105.md`](specs/native-postgres-driver/research/cloud-material_20261105.md), [`audit-findings_20261103.md`](specs/integration-remediation/research/audit-findings_20261103.md), [`google-cloud-rust_20261103.md`](specs/integration-remediation/research/google-cloud-rust_20261103.md)
- **Completed native DuckDB wheel**: [Standalone `vertebra-duckdb` (`vertebra._duckdb`) client, Arrow SHM `Appender`, savepoints, and GCS secret rotation](knowledge/domains/data/database-drivers-and-dialers.md) and [delayed-commit `SyncDuckDBPipeline` / `AsyncDuckDBPipeline`](knowledge/domains/data/query-pipeline-cdc-and-tenancy.md).
- **Completed native MySQL wheel**: [Standalone `vertebra-mysql` (`vertebra._mysql`) client, binary prepared protocol, Cloud SQL IAM, `KILL QUERY` cancel, `VTBARROW` cursors, and delayed-commit pipelines](knowledge/domains/data/database-drivers-and-dialers.md).
- **Completed fail-loud baseline**: [Fail-loud driver guards, codecs, and install hints](knowledge/domains/data/database-drivers-and-dialers.md) and [collaborator-required queue backends](knowledge/domains/framework/distributed-queues-and-email.md).
- **Completed storage**: [Native object storage, bulk transfers, compose, and bounded codecs](knowledge/domains/storage/object-storage.md), [implementation limitations](knowledge/domains/storage/object-storage-limitations.md), [framework consumers](knowledge/domains/extensions/litestar.md), and [persistent TTL envelopes](knowledge/decisions/adr-005-persistent-object-store-ttl-envelopes.md).
- **Completed engine primitives**: [Native pipelines, CDC, and tenant boundaries](knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [adapter-owned providers](knowledge/domains/data/database-drivers-and-dialers.md), and [generic agent consumption](knowledge/domains/agent/mcp-and-a2a-protocol-harness.md).
- **Completed database foundation**: [Driver contracts and adapter layout](knowledge/domains/data/database-drivers-and-dialers.md), [conformance readiness](knowledge/testing/polyglot-test-matrix.md#database-driver-conformance), and [optional wheel ownership](knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md).
- **`research/` (lazy)** — Materialized only when standalone un-scoped technical research briefs exist.
- **[Bundle Activity Log](log.md)** — Chronological log of bundle initialization, syntheses, and archived flows.
