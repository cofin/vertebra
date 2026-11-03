---
type: Spec
flow_id: native-data-platform
title: Native Data Platform (Shared Broker Pools, Rust Drivers, Storage)
description: Master PRD for the committed product features - any Advanced Alchemy/SQLAlchemy or SQLSpec config shares one pool through the shared-memory ring broker, and Vertebra's own Rust drivers and storage back every engine and integration, feature complete.
state: planned
parent_prd: null
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 1
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T04:05:00Z
updated_at: 2026-11-03T04:05:00Z
tags: [prd, drivers, rust, broker, storage, postgres, oracle, mysql, mssql]
---

# Master PRD: Native Data Platform

Baseline: `main @ 142cc49`. Sibling PRD: [integration-remediation](../integration-remediation/spec.md), which consumes this platform.

## 1. North Star Goal & Problem Framing

### Objective
Two features Vertebra has committed to, both feature complete:

1. **Shared pools through the ring broker.** Any existing Advanced Alchemy / SQLAlchemy config or SQLSpec config, including upstream configs such as `sqlspec.adapters.asyncpg.AsyncpgConfig` and AA `SQLAlchemyAsyncConfig`, can route through Vertebra's shared-memory ring broker, so every worker process shares one connection pool.
2. **Vertebra's own high-performance drivers.** Rust drivers back every engine: Postgres, MySQL, Oracle, MSSQL, SQLite, DuckDB, BigQuery, Spanner, Redis/Valkey. Storage and the other data components (Arrow/SHM data plane, ObjectStore, framework storage backends) are equally complete. Every integration (sqlspec adapters, AA/SQLAlchemy dialects, Django backends, queues, channels, stores) runs on these drivers.

### Current state and limits
- **Postgres.** Rust `vertebra-db/src/postgres.rs` only encodes frames (`encode_parse_frame`, `encode_bind_frame`, `encode_execute_frame`, `encode_sync_frame`, pgcopy, pgvector, JSONB) and has a lease/statement-cache pool (`PostgresPool`). There is no connection, auth, TLS, or result decoding. The Python driver executes through `psycopg_pool` (sync) and duck-typed asyncpg (async, where `_ensure_async_pool` returns the module).
- **MySQL.** `mysql.rs` has packet framing, the `caching_sha2` scramble, `COM_QUERY`/`COM_STMT_PREPARE` encoders, and `MysqlPool` leases. The Python driver uses `pymysql`.
- **Oracle.** `oracle.rs` and `tns.rs` have the TTC piggyback, OSON, VECTOR, and TNS descriptor codecs. The Python driver runs on `sqlite3.connect(":memory:")`.
- **MSSQL.** `mssql.rs` has TDS packet/SQL-batch encoders and `MssqlPool` leases. The Python driver runs on `sqlite3.connect(":memory:")`.
- **SQLite and DuckDB.** The Python stdlib `sqlite3` and the `duckdb` module; DuckDB falls back to sqlite3.
- **BigQuery and Spanner.** Real Rust gRPC/REST I/O exists. Wire-format defects are owned by integration-remediation `gcp-wire-protocol-correctness`.
- **Redis/Valkey.** RESP3 codec in `resp3.rs`; the Python driver has a `memory://` mode.
- **Broker.** `vertebra-core/src/broker.rs`, `vertebra-py/src/broker.rs`, `extensions/broker/transport.py::SHMBrokerTransport`/`InMemoryBrokerTransport`, and `extensions/sqlspec/plugin.py::ProxySyncConfig`/`ProxyAsyncConfig` with `proxy.py`/`provider.py`. Gap details are in the chapter `broker-pool-sharing`.

### What is new and why it succeeds
- Each engine gets a complete Rust client in `vertebra-db` (connection, auth, TLS, query/result codecs, prepared statements, transactions, pipelining, cancellation, bulk load, notifications where the engine has them) behind one Python driver contract.
- The broker owns the real connections, and workers lease them over shared memory.
- Upstream configs are wrapped rather than reimplemented, so users keep their existing AA/SQLSpec code and gain pool sharing and Rust I/O.

### Success metrics
- No production driver imports a third-party Python DB client or opens SQLite for a non-SQLite engine.
- Each engine passes the shared driver conformance suite against real servers (`pytest-databases`).
- An unmodified upstream `AsyncpgConfig`, `OracleAsyncConfig`, or AA `SQLAlchemyAsyncConfig` wrapped by Vertebra runs across N workers with one broker-owned pool. Verified by server-side connection counts.
- `driver.pipeline()` sends N statements in one round trip on Postgres and Oracle 23ai.
- `make bench-check` shows the Rust drivers at or above the third-party clients they replace.

### Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Oracle TTC thin protocol is very large (auth O5LOGON, TTC execute/fetch, LOBs, DRCP, pipelining) | Dedicated chapter, sequenced after Postgres; conformance suite defines "complete"; the wire format is checked against python-oracledb Thin captures as golden fixtures |
| MSSQL TDS 7.4 breadth (login7, TLS-in-TDS, token stream, RPC) | Dedicated chapter; golden fixtures from captured sessions |
| Behavior differences versus mature clients | One cross-engine conformance suite plus engine-specific edge suites (NULLs, NUMERIC precision, time zones, LOBs, Unicode) |
| Broker becomes a single point of failure | Supervisor restart plus lease reclamation (`reclaim_worker`) covered by crash tests |

## 2. Locked Architectural Seams

1. **Layout** *(user decision)*.
   - Engine code lives in `vertebra/db/adapters/<engine>/` (`config.py`, `core.py`, `driver.py`, plus engine primitives such as `pipeline.py`, `notify.py`, `queue.py`, `change_stream.py`, `embeddings.py`).
   - Engine-agnostic code lives in `vertebra/db/common/`: driver bases moved from `db/driver/`, capability protocols, the registry, `ArrowCursor`, the broker client, and Cloud SQL/AlloyDB dialers.
   - The Rust crate mirrors this as `vertebra-db/src/<engine>/` modules plus shared `vertebra-db/src/common/`.
2. **Driver contract.** One `SyncDatabaseDriver`/`AsyncDatabaseDriver` contract in `db/common/`:
   - `execute`, `execute_many`, `select*`, `select_to_arrow`, `stream_arrow_batches`, `load_from_arrow`
   - transactions
   - `pipeline()` (`PipelineCapable`)
   - `cancel()`
   - typed exceptions carrying engine codes
   
   Async paths run on the Rust runtime through the shared completion bridge with GIL detach.
3. **Broker ownership.**
   - The broker process owns the real Rust connections per pool key (`BaseDatabaseConfig.pool_key()`).
   - Workers lease a connection over the SHM ring for a session/transaction scope, and statements and results move zero-copy through SHM (VTBARROW for result sets).
   - Upstream SQLSpec configs are wrapped by `ProxySyncConfig`/`ProxyAsyncConfig`. AA/SQLAlchemy configs get a broker-backed SQLAlchemy `Pool` and `creator` through the Vertebra DB-API.
4. **Pipelining.** Implemented natively in the Rust clients: the Postgres extended-protocol batch with a single `Sync`, and Oracle TTC pipelining on 23ai with sequential execution on older servers. This is distinct from the sqlspec `StatementStack`, which uses it.
5. **Storage.** `vertebra-storage` `ObjectStore` (gs/s3/r2/oci/az/file) is the single implementation behind the sqlspec `ObjectStoreProtocol`, the AA `StorageBackend`, Django `Storage`, Litestar static/file responses, and the Arrow export/import paths, with zero-copy streaming and signing.

## 3. Master Roadmap (Ordered Child Flows)

| Ch | Flow ID | Title | Key Deliverables | Depends on | State |
| --- | --- | --- | --- | --- | --- |
| 1 | `db-layout-and-contract` | `db/adapters` + `db/common` layout and driver conformance suite | Package move, driver contract, capability protocols, registry polymorphism, import gate, cross-engine conformance test suite | integration-remediation Ch1 | draft |
| 2 | `native-postgres-driver` | Rust Postgres client | Startup/SCRAM-SHA-256/TLS/IAM, extended protocol, binary codecs, statement cache, COPY, LISTEN/NOTIFY, pipeline mode, cancel, Arrow; psycopg/asyncpg removed | 1 | draft |
| 3 | `broker-pool-sharing` | Shared pools for any SQLSpec / AA / SQLAlchemy config | Broker-owned pools, SHM session leases, transactions/affinity, streaming, `ProxySync/AsyncConfig` for upstream configs, SQLAlchemy broker `Pool`, crash reclamation | 1, 2 | draft |
| 4 | `native-oracle-driver` | Rust Oracle thin client | TNS/O5LOGON/TLS/wallet, TTC execute/fetch, binds/defines, LOBs, OSON/VECTOR, DRCP, pipelining (23ai), Arrow; sqlite3 fake removed | 1 | draft |
| 5 | `native-mysql-mssql-drivers` | Rust MySQL and MSSQL clients | MySQL handshake/caching_sha2/TLS/binary protocol; MSSQL TDS 7.4 prelogin/TLS/login7/RPC/token stream/bulk copy; pymysql and sqlite3 fakes removed | 1 | draft |
| 6 | `native-embedded-and-cache-drivers` | SQLite, DuckDB, Redis/Valkey in Rust | Rust SQLite and DuckDB embeddings with Arrow zero-copy; RESP3 client with cluster, pub/sub, client tracking; `memory://` modes removed | 1 | draft |
| 7 | `storage-completeness` | Feature-complete storage | ObjectStore across backends behind sqlspec/AA/Django/Litestar contracts, zero-copy streaming, ranges, signing, one shared `GcpTokenCache` | 1 | draft |

### Cross-PRD order
1. integration-remediation `critical-correctness-fixes` and `gcp-wire-protocol-correctness`.
2. This PRD's Ch1, then Ch2, then Ch3. Ch4–Ch7 follow; Ch4 and Ch5 can run in parallel.
3. integration-remediation `db-engine-primitives`, `sqlspec-adapter-parity`, `queues-tasks-and-channels` (needs Ch2 and Ch4), `sqlalchemy-and-django-integration`, `mcp-webmcp-core`, and `runtime-structure-and-throughput`.

## 4. Global Constraints

The global constraints from [integration-remediation](../integration-remediation/spec.md) apply. In addition:

- No third-party Python database client in production driver paths.
- Every Rust protocol client ships with golden-byte fixtures captured from real sessions, plus integration tests against real servers.
- `unsafe` blocks carry SAFETY comments.
- Async Rust never calls `Python::attach` on Tokio worker threads.

## 5. Backlog (Deferred)

- AWS RDS/Aurora DSQL IAM ([vertebra#67](https://github.com/cofin/vertebra/issues/67)) and Azure Entra ID ([vertebra#68](https://github.com/cofin/vertebra/issues/68)) auth in the native clients.
- CockroachDB- and AlloyDB-specific protocol features beyond Postgres compatibility.

## Implementation Plan

- [ ] db-layout-and-contract — `db/adapters` + `db/common` layout and driver conformance suite (../db-layout-and-contract/spec.md)
- [ ] native-postgres-driver — Rust Postgres client (../native-postgres-driver/spec.md)
- [ ] broker-pool-sharing — Shared pools for any SQLSpec / AA / SQLAlchemy config (../broker-pool-sharing/spec.md)
- [ ] native-oracle-driver — Rust Oracle thin client (../native-oracle-driver/spec.md)
- [ ] native-mysql-mssql-drivers — Rust MySQL and MSSQL clients (../native-mysql-mssql-drivers/spec.md)
- [ ] native-embedded-and-cache-drivers — SQLite, DuckDB, Redis/Valkey in Rust (../native-embedded-and-cache-drivers/spec.md)
- [ ] storage-completeness — Feature-complete storage (../storage-completeness/spec.md)

## Continuity Snapshot

- **Flow ID:** `native-data-platform` (`state: planned`, `plan_revision: 1`, `plan_commit: null`)
- **Key decisions** *(user, 2026-11-03)*:
  - Shared broker pools for any AA/SQLSpec config and Vertebra's own Rust drivers for every engine are required, feature complete.
  - Storage and other components are complete too.
  - Layout is `db/adapters/<engine>` plus `db/common`.
- **Open item:** broker, driver, and storage gap analysis to be appended to `research/` when the analyst report lands.
