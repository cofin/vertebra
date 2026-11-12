# Active Specifications & Research (`specs/`)

Active specifications and promoted research notes under `.agents/bundles/specs/`. Completed flows are synthesized into [`../knowledge/`](../knowledge/index.md), logged in [`../log.md`](../log.md), and deleted upon `/flow:archive`.

## 1. Master PRDs (`state: planned`)

- [`native-data-platform`](native-data-platform/spec.md) — Shared connection pools across worker processes via the shared-memory ring broker and per-engine standalone Rust PyO3 driver wheels (`vertebra-<engine>`).
- [`integration-remediation`](integration-remediation/spec.md) — Driver layering, SQLSpec adapter parity, SQLAlchemy/Advanced Alchemy/Django integration, native queues/channels/stores, MCP `2026-07-28` core, and runtime fastpath consolidation.

The completed database foundation, fail-loud baseline, and standalone DuckDB, MySQL, SQL Server, and Oracle wheels (`vertebra-duckdb`, `vertebra-mysql`, `vertebra-mssql`, `vertebra-oracle`) are documented in [driver contracts, native clients, and adapter layout](../knowledge/domains/data/database-drivers-and-dialers.md), [VPD tenancy and delayed-commit pipelines](../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [collaborator-required queue backends](../knowledge/domains/framework/distributed-queues-and-email.md), and [ADR-004](../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md).

## 2. Active Feature Specifications (`state: active`)

- [`native-postgres-driver`](native-postgres-driver/spec.md) — Standalone Rust PostgreSQL wheel (`vertebra-postgres`) covering wire authentication, cloud mTLS/IAM, pools, single-Sync pipelines, binary COPY, SHM Arrow, notifications, logical replication, and native adapter cutover (`state: active`; [`36 chapters`](native-postgres-driver/chapters/index.md), `129` tasks, `23/36` chapters completed through `ch23`, `ch34` awaits review with `4/4` verified tasks and `88/129` tasks completed).

- [`mcp-protocol-completeness`](mcp-protocol-completeness/spec.md) — MCP `2026-07-28` JSON-RPC dispatch parity, trusted `AgentContext` propagation, opt-in IAP/mTLS `IdentitySource`, task lifecycle, WebMCP adapter coverage, and standalone skills packaging (`12` chapters, `35` tasks; covers [#114](https://github.com/cofin/vertebra/issues/114), [#115](https://github.com/cofin/vertebra/issues/115) skill split).
- [`server-runtime-correctness`](server-runtime-correctness/spec.md) — Embedded fastpath dispatch cleanup, fork-safe `id.rs`, Rust client `max_frame_size`, FFI batch/SHM hygiene, static root fallback, and typed server metrics (`11` chapters, `33` tasks; covers [#110](https://github.com/cofin/vertebra/issues/110)).
- [`storage-integrity-and-streaming`](storage-integrity-and-streaming/spec.md) — Streaming ObjectStore multipart reads/writes, `sqlspec` upstream Arrow upload tracking, `s3`/`r2` create-only copy guards, and storage packaging checks (`4` chapters, `12` tasks; covers [#99](https://github.com/cofin/vertebra/issues/99), [#100](https://github.com/cofin/vertebra/issues/100)).

## 3. Planned Decomposed Specifications (`state: planned`, awaiting approval)

- [`broker-pool-sharing`](broker-pool-sharing/spec.md) — Shared broker-owned pools for any SQLSpec, Advanced Alchemy, or SQLAlchemy config, cross-cdylib native engine capsules, lease quarantine, `vertebra.db.dbapi` extraction, and PostgreSQL pool reclamation (`9` chapters, `31` tasks; covers [#95](https://github.com/cofin/vertebra/issues/95), [#96](https://github.com/cofin/vertebra/issues/96), [#97](https://github.com/cofin/vertebra/issues/97), [#98](https://github.com/cofin/vertebra/issues/98), [#109](https://github.com/cofin/vertebra/issues/109), [#111](https://github.com/cofin/vertebra/issues/111)).
- [`gcp-embeddings-and-identity`](gcp-embeddings-and-identity/spec.md) — Vertex HTTP/2 transport reliability, WIF/`authorized_user` ADC ID tokens, Spanner/BigQuery correctness and timeout bounds (`6` chapters, `17` tasks).
- [`driver-conformance-ci`](driver-conformance-ci/spec.md) — Real multi-engine database conformance suites, `--group duckdb` benchmark isolation, Oracle AQ/batch/SHM hardening, and CI gates (`5` chapters, `16` tasks; covers [#81](https://github.com/cofin/vertebra/issues/81), [#82](https://github.com/cofin/vertebra/issues/82), [#83](https://github.com/cofin/vertebra/issues/83), [#84](https://github.com/cofin/vertebra/issues/84), [#85](https://github.com/cofin/vertebra/issues/85), [#112](https://github.com/cofin/vertebra/issues/112), [#113](https://github.com/cofin/vertebra/issues/113)).

## 4. Draft Child Flow Charters (`state: draft`)

- [`native-sqlite-driver`](native-sqlite-driver/spec.md) — Standalone Rust SQLite wheel (`vertebra-sqlite`).
- [`native-redis-driver`](native-redis-driver/spec.md) — Standalone Rust Redis and Valkey RESP3 wheel (`vertebra-redis`).
- [`sqlalchemy-and-django-integration`](sqlalchemy-and-django-integration/spec.md) — SQLAlchemy `+vtb` dialect bases, Advanced Alchemy dogpile cache backend, and Django database backends.
- [`sqlspec-adapter-parity`](sqlspec-adapter-parity/spec.md) — Feature-complete SQLSpec per-engine adapters, stores, and data dictionaries.
- [`queues-tasks-and-channels`](queues-tasks-and-channels/spec.md) — Native PostgreSQL and Oracle `QueueStore` backends, `SyncNotificationSource`/`AsyncNotificationSource` splits, `litestar-queues`, Django tasks, distributed channels, and `ValkeyStore`.

## 5. Promoted Spec Research (`state: promoted`)

- [`gap-analysis_20261103.md`](native-data-platform/research/gap-analysis_20261103.md) — Broker, native driver, and storage gap analysis (`native-data-platform`).
- [`rust-driver-crates_20261103.md`](native-data-platform/research/rust-driver-crates_20261103.md) — Primary-source Rust client crate comparison and per-engine wheel packaging (`native-data-platform`).
- [`protocol-and-execution_20261105.md`](native-postgres-driver/research/protocol-and-execution_20261105.md) — PostgreSQL v3.0 extended query, single-Sync pipeline, COPY, and pgoutput v1 wire semantics (`native-postgres-driver`).
- [`cloud-material_20261105.md`](native-postgres-driver/research/cloud-material_20261105.md) — Cloud SQL Admin and AlloyDB ephemeral mTLS certificate and IAM token lifecycle (`native-postgres-driver`).
- [`audit-findings_20261103.md`](integration-remediation/research/audit-findings_20261103.md) — Audit findings across database extensions, framework, GCP drivers, MCP/WebMCP, and `vertebra.db` (`integration-remediation`).
- [`google-cloud-rust_20261103.md`](integration-remediation/research/google-cloud-rust_20261103.md) — Evaluation of official `googleapis/google-cloud-rust` crates for Spanner, BigQuery Storage, and auth (`integration-remediation`).
