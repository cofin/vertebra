---
type: Guide
title: Vertebra Product Definition
description: Product purpose, runtime topologies, and the capabilities that exist in the Python, Rust, and TypeScript packages of the Vertebra application backbone.
status: stable
tags: [product, runtime, python, rust, typescript]
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-archive
    at: "2026-11-14T22:49:00Z"
    commit: ff9f78cb
  - by: agent:flow-executor
    at: "2026-11-14T01:18:00Z"
    commit: 7116ba13
  - by: agent:flow-executor
    at: "2026-11-13T06:06:00Z"
    commit: 05d88d00
sources:
  - id: src-vertebra-mysql-lib-rs
    resource: src/rs/vertebra-mysql/src/lib.rs
    title: "Rust PyO3 extension module (vertebra._mysql)"
  - id: src-vertebra-bigquery-lib-rs
    resource: src/rs/vertebra-bigquery/src/lib.rs
    title: "Rust PyO3 extension module (vertebra._bigquery)"
  - id: src-vertebra-mssql-lib-rs
    resource: src/rs/vertebra-mssql/src/lib.rs
    title: "Rust PyO3 extension module (vertebra._mssql)"
  - id: src-vertebra-spanner-lib-rs
    resource: src/rs/vertebra-spanner/src/lib.rs
    title: "Rust PyO3 extension module (vertebra._spanner)"
  - id: src-vertebra-duckdb-lib-rs
    resource: src/rs/vertebra-duckdb/src/lib.rs
    title: "Rust PyO3 extension module (vertebra._duckdb)"
  - id: src-vertebra-oracle-lib-rs
    resource: src/rs/vertebra-oracle/src/lib.rs
    title: "Rust PyO3 extension module (vertebra._oracle)"
  - id: src-vertebra-postgres-lib-rs
    resource: src/rs/vertebra-postgres/src/lib.rs
    title: "Rust PyO3 extension module (vertebra._postgres)"
  - id: src-vertebra-redis-lib-rs
    resource: src/rs/vertebra-redis/src/lib.rs
    title: "Rust PyO3 extension module (vertebra._redis)"
  - id: src-vertebra-valkey-lib-rs
    resource: src/rs/vertebra-valkey/src/lib.rs
    title: "Rust PyO3 extension module (vertebra._valkey)"
  - id: src-tenant-rls-py
    resource: src/py/vertebra/db/rls.py
    title: "Engine-neutral tenant configuration and ANSI predicates"
  - id: src-tenant-sqlspec-py
    resource: src/py/vertebra/extensions/sqlspec/_driver.py
    title: "Engine-owned session enforcement and unsupported-adapter refusal"
  - id: src-bigquery-tools-py
    resource: src/py/vertebra/db/adapters/bigquery/tools.py
    title: "Adapter-owned BigQuery tool provider"
  - id: src-spanner-tools-py
    resource: src/py/vertebra/db/adapters/spanner/tools.py
    title: "Adapter-owned Spanner tool provider"
  - id: src-agent-tools-py
    resource: src/py/vertebra/agent/tools/__init__.py
    title: "Generic tool-provider bridge"
  - id: src-readme-md
    resource: README.md
    title: "Repository overview and quickstart documentation"
  - id: src-pyproject-toml
    resource: pyproject.toml
    title: "Python package metadata, dependencies, and tool configuration"
  - id: src-init-py
    resource: src/py/vertebra/__init__.py
    title: "Python implementation module (src/py/vertebra/__init__.py)"
  - id: src-framework-init-py
    resource: src/py/vertebra/framework/__init__.py
    title: "Python implementation module (src/py/vertebra/framework/__init__.py)"
  - id: src-storage-init-py
    resource: src/py/vertebra/storage/__init__.py
    title: "Python implementation module (src/py/vertebra/storage/__init__.py)"
  - id: src-db-init-py
    resource: src/py/vertebra/db/__init__.py
    title: "Python implementation module (src/py/vertebra/db/__init__.py)"
  - id: src-telemetry-init-py
    resource: src/py/vertebra/telemetry/__init__.py
    title: "Python implementation module (src/py/vertebra/telemetry/__init__.py)"
  - id: src-server-platform-py
    resource: src/py/vertebra/server/platform.py
    title: "Python implementation module (src/py/vertebra/server/platform.py)"
  - id: src-wasm-init-py
    resource: src/py/vertebra/wasm/__init__.py
    title: "Python implementation module (src/py/vertebra/wasm/__init__.py)"
  - id: src-cli-init-py
    resource: src/py/vertebra/cli/__init__.py
    title: "Python implementation module (src/py/vertebra/cli/__init__.py)"
  - id: src-cargo-toml
    resource: src/rs/Cargo.toml
    title: "Rust crate manifest (src/rs/Cargo.toml)"
  - id: src-package-json
    resource: src/ts/vertebra-client/package.json
    title: "TypeScript package manifest (src/ts/vertebra-client/package.json)"
  - id: src-src-index-ts
    resource: src/ts/vertebra-client/src/index.ts
    title: "TypeScript client module (src/ts/vertebra-client/src/index.ts)"
---

# Vertebra Product Definition

## Project Purpose

Vertebra is a Rust-backed runtime and application framework for Python and TypeScript covering web APIs, gRPC, frontend bundling, background workers, and standalone binaries. Its Rust HTTP ingress and IPC core hosts Python handlers (native, ASGI, RSGI, and WSGI) either in-process or across shared-memory worker processes.

Around that runtime sit a Rust object-storage extension, database and cache drivers, telemetry, platform diagnostics, live state synchronization, framework extensions, the `vtb` CLI, and the `@vertebra/client` TypeScript SDK.

[`vertebra/__init__.py`](../../../src/py/vertebra/__init__.py)[^src-init-py] and [`vertebra/framework/__init__.py`](../../../src/py/vertebra/framework/__init__.py)[^src-framework-init-py] declare PEP 810 `__lazy_modules__` alongside PEP 562 `__getattr__`, so `import vertebra` defers loading framework, server, and native extension modules until first attribute access across Python 3.12–3.15. CI targets Linux and macOS; `vertebra-core` also carries a Windows platform module ([`platform/windows.rs`](../../../src/rs/vertebra-core/src/platform/windows.rs)).

## Core Product Capabilities

<!-- truth: start -->
- **Python Framework (`vertebra.framework`)**: [`App`](../../../src/py/vertebra/framework/_app.py), [`Controller`](../../../src/py/vertebra/framework/_router.py), [`Router`](../../../src/py/vertebra/framework/_router.py), [`Procedure`](../../../src/py/vertebra/framework/procedures.py), and hierarchical DI ([`di.py`](../../../src/py/vertebra/framework/di.py)) with typed parameter extraction, opt-in `sync_to_thread` offloading, sub-app mounting, Inertia.js middleware ([`inertia/__init__.py`](../../../src/py/vertebra/framework/inertia/__init__.py)), SSR, OpenAPI/AsyncAPI portals, and 14 subdomain packages; `a2a` and `mcp` live under [`vertebra.agent`](../../../src/py/vertebra/agent/__init__.py).
- **Build, Codegen & Packaging (`vertebra.build`)**: [`BuildOrchestrator`](../../../src/py/vertebra/build/_orchestrator.py) and [`TypeExtractor`](../../../src/py/vertebra/build/codegen/_extractor.py) generate TypeScript, Zod, Valibot, OpenAPI, bidirectional Protobuf v3 ([`protobuf.py`](../../../src/py/vertebra/build/codegen/protobuf.py)), and typed procedure clients. `build/packaging/` produces standalone directories and self-extracting single-file executables (`vtb build --standalone|--onefile`), while `vertebra-core` wheels bundle a relocatable `vertebra-server` binary.
- **Multi-Protocol Execution**: Hosts native handlers (zero-copy `Request.body_view`, `decode_msgspec`, progressive streaming), ASGI 3.0 ([`AsgiRunner`](../../../src/py/vertebra/server/_asgi/_runner.py), [`asgi.rs`](../../../src/rs/vertebra-py/src/asgi.rs)), RSGI 1.6 ([`RsgiAdapter`](../../../src/py/vertebra/server/_rsgi/_adapter.py), [`rsgi/scope.rs`](../../../src/rs/vertebra-py/src/rsgi/scope.rs)), and PEP 3333 WSGI ([`WsgiAdapter`](../../../src/py/vertebra/server/_wsgi/_adapter.py), [`wsgi/environ.rs`](../../../src/rs/vertebra-py/src/wsgi/environ.rs)) selected via `vtb run --interface`.
- **Runtime Topologies**: `vtb run --topology auto|embedded|shm` selects between in-process embedded execution and multi-process shared-memory workers:
  - **Embedded**: `vertebra-server` embeds CPython via `append_to_inittab!(pymodule_init)` in [`main.rs`](../../../src/rs/vertebra-http/src/main.rs), dispatches `InProcessRequest` batches over [`BatchBridge`](../../../src/rs/vertebra-core/src/bridge.rs) to [`worker.py`](../../../src/py/vertebra/server/worker.py), and supervises `SO_REUSEPORT` worker processes on Unix when `workers > 1`.
  - **Shared Memory (`shm`)**: Auto-selected on POSIX GIL Python when `workers > 1`; [`RingFactory`](../../../src/rs/vertebra-http/src/workers/factory.rs) spawns worker processes reading from [`MpmcRing`](../../../src/rs/vertebra-core/src/channel.rs) and writing responses across [`TieredRingSet`](../../../src/rs/vertebra-core/src/transport/ring_pair.rs) tiers (`46 MiB/worker` default, `12 MiB/worker` compact) with `/tmp/vtb-*` overflow fallback.
  - **Free-Threaded Python (`3.14t`, `3.15t`)**: All 13 PyO3 extensions declare `#[pymodule(gil_used = false)]`, disable mutex-guarded freelists under `Py_GIL_DISABLED`, freeze non-subclassed types, and run multi-thread embedded worker pools.
- **Native HTTP (`vertebra-http`)**: Hyper HTTP/1.1 and HTTP/2, opt-in h2c, `rustls` TLS/mTLS, optional HTTP/3 over QUIC, lock-free [`BackpressureController`](../../../src/rs/vertebra-http/src/backpressure.rs), and Rust offloads for health, CORS, compression, static assets, OpenAPI cache, HMR WebSockets, gRPC framing, SSE/WebSocket pub/sub, `.well-known` discovery, and multipart parsing.
- **Bundling & SSR (`vertebra-bundler`)**: Oxc TypeScript/JSX transforms ([`bundler.rs`](../../../src/rs/vertebra-bundler/src/bundler.rs)), `rolldown-notify` HMR file watching, and pooled QuickJS SSR ([`ssr.rs`](../../../src/rs/vertebra-bundler/src/ssr.rs)).
- **Object Storage (`vertebra-storage`, `vertebra.storage`)**: Wraps `object_store` (S3, GCS, Azure, HTTP) plus native OCI REST ([`oci.rs`](../../../src/rs/vertebra-storage/src/oci.rs)) and multicloud presigning ([`signing.rs`](../../../src/rs/vertebra-storage/src/signing.rs)) behind [`SyncObjectStore` and `AsyncObjectStore`](../../../src/py/vertebra/storage/_store.py), sharing GCP token caches via C-ABI capsule, conditional writes, bulk transfers, streaming files, Arrow/Parquet/CSV codecs, and HTTP Range responses.
- **Data Access (`vertebra.db`, `vertebra-db`)**: Shared-memory connection broker and standalone Rust driver wheels for PostgreSQL, MySQL, SQLite, Oracle, MSSQL, DuckDB, BigQuery, Spanner, Redis, and Valkey, paired with Cloud SQL/AlloyDB/AWS/Azure dialers, zero-copy `ArrowCursor` transport, native pipelines, CDC `ChangeFeed`, `TenantPredicate`, `DataFusionEngine`, and `DatabaseRegistry`.
- **Telemetry, Platform, Live State & WASM**: Async Rust logging ([`logger.rs`](../../../src/rs/vertebra-core/src/logger.rs)) and W3C tracing ([`telemetry.rs`](../../../src/rs/vertebra-core/src/telemetry.rs)); cgroup sizing, lame-duck drain, and ZPages ([`platform.py`](../../../src/py/vertebra/server/platform.py)[^src-server-platform-py]); MessagePack LWW live state ([`live.py`](../../../src/py/vertebra/framework/live.py)); and sandboxed WASM host validation (`vertebra.wasm`, no engine bundled).
- **Framework Extensions (`vertebra.extensions`)**: Integrations for Litestar, Starlette, FastAPI, Flask, Django (including 8 native database backends), SQLSpec, Advanced Alchemy, SQLAlchemy `+vtb` dialects and Alembic hooks, SHM connection broker, gRPC, and Oracle APEX.
- **CLI (`vtb`) & TypeScript SDK (`@vertebra/client`)**: Pluggable Click/Rich CLI ([`cli/__init__.py`](../../../src/py/vertebra/cli/__init__.py)[^src-cli-init-py], [`plugin_registry.py`](../../../src/py/vertebra/cli/plugin_registry.py)) covering `run`, `dev`, `init`, `js`, `build`, `ai`, `workers`, and `broker`; multi-entry browser SDK ([`client.ts`](../../../src/ts/vertebra-client/src/client/client.ts), [`websocket.ts`](../../../src/ts/vertebra-client/src/websocket.ts), [`store/index.ts`](../../../src/ts/vertebra-client/src/store/index.ts), [`hmr/index.ts`](../../../src/ts/vertebra-client/src/hmr/index.ts)) with adapters for React, Vue, Svelte, Angular, HTMX, and Vite.
<!-- truth: end -->

## Tenancy & Agent Tools

Tenant configuration is engine-neutral (`setting_name`, optional parameter cast, caller-selected placeholder)[^src-tenant-rls-py]. PostgreSQL and Oracle cores own session SQL with reset-or-evict pooling on Oracle drivers; unsupported SQLSpec adapters refuse session enforcement[^src-tenant-sqlspec-py], and BigQuery and Spanner reject configured predicates before initialization.

Adapter-owned `BigQueryToolProvider`[^src-bigquery-tools-py] and `SpannerToolProvider`[^src-spanner-tools-py] expose typed operations adapted into `Tool` instances with shared confirmation gating by `vertebra.agent.tools.build_tools`[^src-agent-tools-py].

## Architectural Overview

| Layer | Package / Crate | Primary Responsibility |
| --- | --- | --- |
| Python Framework & Server | [`src/py/vertebra/`](../../../src/py/vertebra/__init__.py) | `App`, routing, DI, subdomains, codegen/packaging, ASGI/RSGI/WSGI runners, `EventLoopPool`, multi-lane workers, `vtb` CLI, and supervisor launcher |
| Python Data, Storage & Telemetry | [`storage/`](../../../src/py/vertebra/storage/__init__.py)[^src-storage-init-py], [`db/`](../../../src/py/vertebra/db/__init__.py)[^src-db-init-py], [`telemetry/`](../../../src/py/vertebra/telemetry/__init__.py)[^src-telemetry-init-py] | Object storage facades, database configs/dialers/drivers, `ArrowCursor`, pipelines, CDC, logging, and tracing |
| Python Extensions | [`src/py/vertebra/extensions/`](../../../src/py/vertebra/extensions/__init__.py) | Litestar, Starlette, FastAPI, Flask, Django, SQLSpec, Advanced Alchemy, SQLAlchemy `+vtb` dialects, broker, gRPC, and Oracle APEX |
| Rust Core IPC & Wire Protocol | [`src/rs/vertebra-core/`](../../../src/rs/vertebra-core/src/lib.rs) | Shared-memory regions, SPSC/MPMC rings, `BatchBridge`, `RingPair`/`TieredRingSet`, `ShmLeaseTable` + statement cache, compression, OS wakeups, `gcp_auth`, logging, tracing, JWT, and rate limiting |
| Rust HTTP Ingress & Dispatch | [`src/rs/vertebra-http/`](../../../src/rs/vertebra-http/src/lib.rs) | `vertebra-server` binary, accept loop (`ServerState`), `matchit` radix router, `RequestBatcher`, `Dispatcher`/`ShmDispatcher`, `BackpressureController`, and `RingFactory` |
| Rust Python FFI Extension | [`src/rs/vertebra-py/`](../../../src/rs/vertebra-py/src/lib.rs) | PyO3 `vertebra._vertebra`: zero-copy request/response/scope/environ types, C-API `msgspec_bridge`, batch drivers, `LeaseBroker`, cloud dialers, GCP token caches, `ArrowCursor`, and `LogSink` |
| Rust Native Bundler & SSR | [`src/rs/vertebra-bundler/`](../../../src/rs/vertebra-bundler/src/lib.rs) | PyO3 `vertebra._bundler`: Oxc TS/JSX transforms, `SourceWatcher`, and QuickJS `SSRPool` |
| Rust Object Storage | [`src/rs/vertebra-storage/`](../../../src/rs/vertebra-storage/src/lib.rs) | PyO3 `vertebra._storage`: `object_store` backends, native OCI REST, GCS compose, streaming readers/writers, and presigning |
| Rust Database & Wire Core | [`src/rs/vertebra-db/`](../../../src/rs/vertebra-db/src/lib.rs) | Shared transport streams, cloud dialer URIs, pool budgets, wire codecs, and `VTBARROW` SHM framing |
| Rust Relational Driver Wheels | [`mysql`](../../../src/rs/vertebra-mysql/src/lib.rs)[^src-vertebra-mysql-lib-rs], [`mssql`](../../../src/rs/vertebra-mssql/src/lib.rs)[^src-vertebra-mssql-lib-rs], [`oracle`](../../../src/rs/vertebra-oracle/src/lib.rs)[^src-vertebra-oracle-lib-rs], [`postgres`](../../../src/rs/vertebra-postgres/src/lib.rs)[^src-vertebra-postgres-lib-rs], [`duckdb`](../../../src/rs/vertebra-duckdb/src/lib.rs)[^src-vertebra-duckdb-lib-rs] | Standalone PyO3 extensions (`_mysql`, `_mssql`, `_oracle`, `_postgres`, `_duckdb`; plus `_sqlite`) with native wire protocols, pools, pipelines, and `VTBARROW` SHM transport |
| Rust Cloud & Cache Driver Wheels | [`bigquery`](../../../src/rs/vertebra-bigquery/src/lib.rs)[^src-vertebra-bigquery-lib-rs], [`spanner`](../../../src/rs/vertebra-spanner/src/lib.rs)[^src-vertebra-spanner-lib-rs], [`redis`](../../../src/rs/vertebra-redis/src/lib.rs)[^src-vertebra-redis-lib-rs], [`valkey`](../../../src/rs/vertebra-valkey/src/lib.rs)[^src-vertebra-valkey-lib-rs] | Standalone PyO3 extensions (`_bigquery`, `_spanner`, `_redis`, `_valkey`) for GCP analytics/distributed SQL and RESP3 caching, Sentinel, Cluster, and Streams |
| TypeScript Client SDK | [`src/ts/vertebra-client/`](../../../src/ts/vertebra-client/src/index.ts)[^src-src-index-ts] | `@vertebra/client` multi-entry browser client and framework adapters |

## Related Context

- [Product Guidelines](product-guidelines.md)
- [Tech Stack](tech-stack.md)
- [Knowledge Index](../knowledge/index.md)

Provenance sources: [^src-readme-md], [^src-pyproject-toml], [^src-wasm-init-py], [^src-cargo-toml], [^src-package-json].

[^src-vertebra-mysql-lib-rs]: `src/rs/vertebra-mysql/src/lib.rs` — Rust PyO3 extension module (vertebra._mysql).
[^src-vertebra-bigquery-lib-rs]: `src/rs/vertebra-bigquery/src/lib.rs` — Rust PyO3 extension module (vertebra._bigquery).
[^src-vertebra-mssql-lib-rs]: `src/rs/vertebra-mssql/src/lib.rs` — Rust PyO3 extension module (vertebra._mssql).
[^src-vertebra-spanner-lib-rs]: `src/rs/vertebra-spanner/src/lib.rs` — Rust PyO3 extension module (vertebra._spanner).
[^src-vertebra-duckdb-lib-rs]: `src/rs/vertebra-duckdb/src/lib.rs` — Rust PyO3 extension module (vertebra._duckdb).
[^src-vertebra-oracle-lib-rs]: `src/rs/vertebra-oracle/src/lib.rs` — Rust PyO3 extension module (vertebra._oracle).
[^src-vertebra-postgres-lib-rs]: `src/rs/vertebra-postgres/src/lib.rs` — Rust PyO3 extension module (vertebra._postgres).
[^src-vertebra-redis-lib-rs]: `src/rs/vertebra-redis/src/lib.rs` — Rust PyO3 extension module (vertebra._redis).
[^src-vertebra-valkey-lib-rs]: `src/rs/vertebra-valkey/src/lib.rs` — Rust PyO3 extension module (vertebra._valkey).
[^src-tenant-rls-py]: `src/py/vertebra/db/rls.py` — Engine-neutral tenant configuration and ANSI predicates.
[^src-tenant-sqlspec-py]: `src/py/vertebra/extensions/sqlspec/_driver.py` — Engine-owned session enforcement and unsupported-adapter refusal.
[^src-bigquery-tools-py]: `src/py/vertebra/db/adapters/bigquery/tools.py` — Adapter-owned BigQuery tool provider.
[^src-spanner-tools-py]: `src/py/vertebra/db/adapters/spanner/tools.py` — Adapter-owned Spanner tool provider.
[^src-agent-tools-py]: `src/py/vertebra/agent/tools/__init__.py` — Generic tool-provider bridge.
[^src-readme-md]: `README.md` — Repository overview and quickstart documentation.
[^src-pyproject-toml]: `pyproject.toml` — Python package metadata, dependencies, and tool configuration.
[^src-init-py]: `src/py/vertebra/__init__.py` — Python implementation module (src/py/vertebra/__init__.py).
[^src-framework-init-py]: `src/py/vertebra/framework/__init__.py` — Python implementation module (src/py/vertebra/framework/__init__.py).
[^src-storage-init-py]: `src/py/vertebra/storage/__init__.py` — Python implementation module (src/py/vertebra/storage/__init__.py).
[^src-db-init-py]: `src/py/vertebra/db/__init__.py` — Python implementation module (src/py/vertebra/db/__init__.py).
[^src-telemetry-init-py]: `src/py/vertebra/telemetry/__init__.py` — Python implementation module (src/py/vertebra/telemetry/__init__.py).
[^src-server-platform-py]: `src/py/vertebra/server/platform.py` — Python implementation module (src/py/vertebra/server/platform.py).
[^src-wasm-init-py]: `src/py/vertebra/wasm/__init__.py` — Python implementation module (src/py/vertebra/wasm/__init__.py).
[^src-cli-init-py]: `src/py/vertebra/cli/__init__.py` — Python implementation module (src/py/vertebra/cli/__init__.py).
[^src-cargo-toml]: `src/rs/Cargo.toml` — Rust crate manifest (src/rs/Cargo.toml).
[^src-package-json]: `src/ts/vertebra-client/package.json` — TypeScript package manifest (src/ts/vertebra-client/package.json).
[^src-src-index-ts]: `src/ts/vertebra-client/src/index.ts` — TypeScript client module (src/ts/vertebra-client/src/index.ts).
