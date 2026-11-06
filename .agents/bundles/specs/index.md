---
title: "Active Specifications & Research Index"
description: "Index of active Master PRDs, planned feature specifications, draft child flow charters, and spec-scoped primary-source research under .agents/bundles/specs/."
tags:
  - specs
  - prd
  - research
  - index
---

# Active Specifications & Research (`specs/`)

Active specifications and promoted research notes under `.agents/bundles/specs/`. Completed flows are synthesized into [`../knowledge/`](../knowledge/index.md), logged in [`../log.md`](../log.md), and deleted upon `/flow:archive`.

## 1. Master PRDs (`state: planned`)

- [`native-data-platform`](native-data-platform/spec.md) — Shared connection pools across worker processes via the shared-memory ring broker and per-engine standalone Rust PyO3 driver wheels (`vertebra-<engine>`).
- [`integration-remediation`](integration-remediation/spec.md) — Driver layering, SQLSpec adapter parity, SQLAlchemy/Advanced Alchemy/Django integration, native queues/channels/stores, MCP `2026-07-28` core, and runtime fastpath consolidation.

The completed database foundation is documented in [driver contracts and adapter layout](../knowledge/domains/data/database-drivers-and-dialers.md) and [ADR-004](../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md).

## 2. Active Feature Specifications (`state: active`)

No active feature specification. Completed engine primitives are documented in the [data chapter](../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md) and [archive log](../log.md).

## 3. Draft Child Flow Charters (`state: draft`)

- [`native-postgres-driver`](native-postgres-driver/spec.md) — Standalone Rust PostgreSQL wheel (`vertebra-postgres`).
- [`broker-pool-sharing`](broker-pool-sharing/spec.md) — Shared broker-owned pools for any SQLSpec, Advanced Alchemy, or SQLAlchemy config.
- [`native-oracle-driver`](native-oracle-driver/spec.md) — Standalone Rust Oracle Thin wheel (`vertebra-oracle`).
- [`native-mysql-mssql-drivers`](native-mysql-mssql-drivers/spec.md) — Standalone Rust MySQL and MSSQL wheels (`vertebra-mysql`, `vertebra-mssql`).
- [`native-embedded-and-cache-drivers`](native-embedded-and-cache-drivers/spec.md) — Standalone Rust SQLite, DuckDB, and Redis/Valkey wheels (`vertebra-sqlite`, `vertebra-duckdb`, `vertebra-redis`).
- [`storage-completeness`](storage-completeness/spec.md) — Complete `SyncObjectStore` and `AsyncObjectStore` (`vertebra-storage`) across SQLSpec, Advanced Alchemy, Django, and Litestar.
- [`sqlspec-adapter-parity`](sqlspec-adapter-parity/spec.md) — Feature-complete SQLSpec per-engine adapters, stores, and data dictionaries.
- [`queues-tasks-and-channels`](queues-tasks-and-channels/spec.md) — Native PostgreSQL and Oracle `QueueStore` backends, `litestar-queues`, Django tasks, distributed channels, and `ValkeyStore`.
- [`sqlalchemy-and-django-integration`](sqlalchemy-and-django-integration/spec.md) — SQLAlchemy `+vtb` dialect bases, Advanced Alchemy dogpile cache backend, and Django database backends.
- [`mcp-webmcp-core`](mcp-webmcp-core/spec.md) — Unified `ToolSpec` / `ToolRegistry` / `MCPCore` (`2026-07-28`), `litestar-mcp` delegation, and `document.modelContext` WebMCP bridge.
- [`runtime-structure-and-throughput`](runtime-structure-and-throughput/spec.md) — Shared `_fastpath_core.py`, table-driven loop backends, Rust GCP submodule splits, `VertexEmbedder` HTTP/2 transport, and `vertebra.utils` Rust helper cleanup.

## 4. Promoted Spec Research (`state: promoted`)

- [`gap-analysis_20261103.md`](native-data-platform/research/gap-analysis_20261103.md) — Broker, native driver, and storage gap analysis (`native-data-platform`).
- [`rust-driver-crates_20261103.md`](native-data-platform/research/rust-driver-crates_20261103.md) — Primary-source Rust client crate comparison and per-engine wheel packaging (`native-data-platform`).
- [`audit-findings_20261103.md`](integration-remediation/research/audit-findings_20261103.md) — Audit findings across database extensions, framework, GCP drivers, MCP/WebMCP, and `vertebra.db` (`integration-remediation`).
- [`google-cloud-rust_20261103.md`](integration-remediation/research/google-cloud-rust_20261103.md) — Evaluation of official `googleapis/google-cloud-rust` crates for Spanner, BigQuery Storage, and auth (`integration-remediation`).
