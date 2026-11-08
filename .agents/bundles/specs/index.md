# Active Specifications & Research (`specs/`)

Active specifications and promoted research notes under `.agents/bundles/specs/`. Completed flows are synthesized into [`../knowledge/`](../knowledge/index.md), logged in [`../log.md`](../log.md), and deleted upon `/flow:archive`.

## 1. Master PRDs (`state: planned`)

- [`native-data-platform`](native-data-platform/spec.md) — Shared connection pools across worker processes via the shared-memory ring broker and per-engine standalone Rust PyO3 driver wheels (`vertebra-<engine>`).
- [`integration-remediation`](integration-remediation/spec.md) — Driver layering, SQLSpec adapter parity, SQLAlchemy/Advanced Alchemy/Django integration, native queues/channels/stores, MCP `2026-07-28` core, and runtime fastpath consolidation.

The completed database foundation is documented in [driver contracts and adapter layout](../knowledge/domains/data/database-drivers-and-dialers.md) and [ADR-004](../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md).

## 2. Active Feature Specifications (`state: active`)

- [`storage-completeness`](storage-completeness/spec.md) — Complete `SyncObjectStore` and `AsyncObjectStore` (`vertebra-storage`), unified GCP token caches, fail-closed construction, and SQLSpec, Advanced Alchemy, Django, Litestar, and `VTBARROW` storage contracts ([`chapters/ch01`–`ch04`](storage-completeness/chapters/index.md), `12` tasks).

- [`native-postgres-driver`](native-postgres-driver/spec.md) — Standalone Rust PostgreSQL wheel (`vertebra-postgres`) covering wire authentication, cloud mTLS/IAM, pools, single-Sync pipelines, binary COPY, SHM Arrow, notifications, logical replication, and native adapter cutover ([`chapters/ch01`–`ch31`](native-postgres-driver/chapters/index.md), `111` tasks). Progress is `9/111` tasks complete and `2/31` chapters complete and reviewed, most recently chapter 02 at `27b5a9e5`. Chapter 03 ordered SCRAM is verified at `9b94bff8`, with peer-certificate binding in progress.

## 3. Draft Child Flow Charters (`state: draft`)

- [`broker-pool-sharing`](broker-pool-sharing/spec.md) — Shared broker-owned pools for any SQLSpec, Advanced Alchemy, or SQLAlchemy config.
- [`native-oracle-driver`](native-oracle-driver/spec.md) — Standalone Rust Oracle Thin wheel (`vertebra-oracle`).
- [`native-mysql-driver`](native-mysql-driver/spec.md) — Standalone Rust MySQL and MariaDB wheel (`vertebra-mysql`).
- [`native-mssql-driver`](native-mssql-driver/spec.md) — Standalone Rust SQL Server TDS wheel (`vertebra-mssql`).
- [`native-sqlite-driver`](native-sqlite-driver/spec.md) — Standalone Rust SQLite wheel (`vertebra-sqlite`).
- [`native-duckdb-driver`](native-duckdb-driver/spec.md) — Standalone Rust DuckDB wheel (`vertebra-duckdb`).
- [`native-redis-driver`](native-redis-driver/spec.md) — Standalone Rust Redis and Valkey RESP3 wheel (`vertebra-redis`).
- [`sqlalchemy-and-django-integration`](sqlalchemy-and-django-integration/spec.md) — SQLAlchemy `+vtb` dialect bases, Advanced Alchemy dogpile cache backend, and Django database backends.
- [`sqlspec-adapter-parity`](sqlspec-adapter-parity/spec.md) — Feature-complete SQLSpec per-engine adapters, stores, and data dictionaries.
- [`queues-tasks-and-channels`](queues-tasks-and-channels/spec.md) — Native PostgreSQL and Oracle `QueueStore` backends, `litestar-queues`, Django tasks, distributed channels, and `ValkeyStore`.

## 4. Promoted Spec Research (`state: promoted`)

- [`gap-analysis_20261103.md`](native-data-platform/research/gap-analysis_20261103.md) — Broker, native driver, and storage gap analysis (`native-data-platform`).
- [`rust-driver-crates_20261103.md`](native-data-platform/research/rust-driver-crates_20261103.md) — Primary-source Rust client crate comparison and per-engine wheel packaging (`native-data-platform`).
- [`protocol-and-execution_20261105.md`](native-postgres-driver/research/protocol-and-execution_20261105.md) — PostgreSQL v3.0 extended query, single-Sync pipeline, COPY, and pgoutput v1 wire semantics (`native-postgres-driver`).
- [`cloud-material_20261105.md`](native-postgres-driver/research/cloud-material_20261105.md) — Cloud SQL Admin and AlloyDB ephemeral mTLS certificate and IAM token lifecycle (`native-postgres-driver`).
- [`audit-findings_20261103.md`](integration-remediation/research/audit-findings_20261103.md) — Audit findings across database extensions, framework, GCP drivers, MCP/WebMCP, and `vertebra.db` (`integration-remediation`).
- [`google-cloud-rust_20261103.md`](integration-remediation/research/google-cloud-rust_20261103.md) — Evaluation of official `googleapis/google-cloud-rust` crates for Spanner, BigQuery Storage, and auth (`integration-remediation`).
