---
type: Spec
flow_id: db-postgres-and-cloud-dialers
title: "Chapter 1: PostgreSQL drivers, Cloud SQL and AlloyDB mTLS dialers, and postgresql+vtb dialect"
state: planned
plan_revision: 2
plan_commit: ""
state_revision: 0
current_task: null
last_operation: null
operation_targets: []
last_verified_checkpoint: null
created_at: 2026-10-22T04:45:00Z
updated_at: 2026-10-22T04:45:00Z
description: "Implement pooled SyncPostgresDriver and AsyncPostgresDriver with Arrow binary COPY streaming, live CloudSqlDialer and AlloyDbDialer mTLS certificate exchange, and the SQLAlchemy 2.1 PostgresVtbDialect (postgresql+vtb)."
tags: [feature, database, postgres, cloudsql, alloydb, sqlalchemy]
parent_prd: vertebra-native-db-drivers
research: [../vertebra-design-quality/findings.md]
---

# Flow: Chapter 1: PostgreSQL drivers, Cloud SQL and AlloyDB mTLS dialers, and postgresql+vtb dialect

**Flow ID:** `db-postgres-and-cloud-dialers`

## Specification

### Parent Context

This chapter is Chapter 1 of [`vertebra-native-db-drivers`](../vertebra-native-db-drivers/spec.md). It owns requirements **R1** (production PostgreSQL sync/async drivers and Arrow binary `COPY` streaming), **R2** (zero-sidecar Cloud SQL `:3307` and AlloyDB `:5433` mTLS dialers with IAM token and ephemeral certificate exchange), **R3** (the PostgreSQL portion of SQLAlchemy 2.1 `+vtb` dialect registration, `Vector`, `BinaryJson`, `RETURNING` sentinels, and `DELETE ... USING`), and **R9** (PostgreSQL + dialer + dialect integration suite). It has no prior chapter dependencies inside `vertebra-native-db-drivers`.

### Code Analysis Summary

- **`src/py/vertebra/db/postgres.py` (173 lines):**
  - `PostgresConfig` (`:23-107`) defines pool bounds, `pooler_mode` (`"direct"`, `"transaction"`, `"session"`), `effective_statement_cache_size` (`:49-57`), `effective_direct_dsn` (`:59-62`), `compute_worker_pool_budget` (`:65-80`, which has duplicate `reserved_super_connections` and `reserved_admin_connections` parameters), `build_session_gucs` (`:81-91`), and `to_cloud_dialer_config` (`:93-106`).
  - `SyncPostgresDriver` (`:109-147`) holds only `_config` and `_in_memory_rows`; `execute_tuples` (`:135-143`) discards `sql` and `parameters` and returns `list(self._in_memory_rows)`.
  - `AsyncPostgresDriver` (`:149-173`) wraps `SyncPostgresDriver` and runs `self._sync.execute_tuples` synchronously (`:163-169`).
- **`src/py/vertebra/db/dialer/cloudsql.py` (113 lines):**
  - `CloudSqlInstanceUri` (`:17-45`) parses `project:region:instance` and builds `generate_ephemeral_cert_url` (`:41-44`).
  - `CloudSqlConfig` (`:47-56`) defines `refresh_Lead_seconds: int = 240` (`:54`, miscased capital `L`).
  - `CloudSqlDialer` (`:58-113`) tracks `_cached_token` and `_cached_expiry`, but `build_tls_context` (`:90-113`) raises `NotImplementedError` (`:112`) when `client_cert_pem` and `client_key_pem` are supplied, and there is no ephemeral keypair generation, Admin API HTTP client call, or socket `connect_sync` / `connect_async` method.
- **`src/py/vertebra/db/dialer/alloydb.py` (124 lines):**
  - `AlloyDbClusterUri` (`:25-57`) parses `projects/<p>/locations/<l>/clusters/<c>/instances/<i>` and builds `generate_client_cert_url` (`:53-56`).
  - `AlloyDbDialer` (`:70-124`) encodes the 4-byte length-prefixed `MetadataExchangeRequest` protobuf wire frame (`:95-114`), but has no `decode_metadata_exchange_response`, TLS context builder, certificate exchange client, or socket factory.
- **`src/py/vertebra/extensions/sqlalchemy/dialects.py` (291 lines) & `pyproject.toml:22-30`:**
  - `pyproject.toml:23-24` registers entry points `"postgresql.vertebra"` and `"postgresql.vtb"` pointing to `PostgresVertebraDialect`, which does not exist in `dialects.py`.
  - `DBAPIConnection` (`:101-150`) only instantiates `SyncSqliteDriver` (`:109-111`) and returns `[]` for PostgreSQL URLs (`:136`).
  - `PostgresDialect` (`:225-233`) is a `DialectDescriptor` struct instance rather than a SQLAlchemy `DefaultDialect` subclass.

#### Findings carried in this chapter

- `FW-01` (PostgreSQL portion): Replace no-op `SyncPostgresDriver.execute_tuples` and `AsyncPostgresDriver.execute_tuples` with real `psycopg` v3 / `asyncpg` pooled execution.
- `FW-04`: Complete `CloudSqlDialer.build_tls_context` client PEM loading and implement live mTLS dialing in `CloudSqlDialer` and `AlloyDbDialer`.
- `FW-44` (PostgreSQL dialect portion): Provide real SQLAlchemy 2.1 `PostgresVtbDialect` subclassing `DefaultDialect` / `PGDialect` with `Vector`, `BinaryJson`, `RETURNING` sentinel, and `DELETE ... USING` support.
- `FW-54`: Rename `CloudSqlConfig.refresh_Lead_seconds` to `refresh_lead_seconds`.
- `FW-55`: Remove duplicate `reserved_super_connections` parameter from `PostgresConfig.compute_worker_pool_budget`.

### Relevant Patterns

- [Product Guidelines](../../product/product-guidelines.md) — `msgspec.Struct(frozen=True, gc=False)`, zero inline `#` comments, `vertebra.utils.serializers` for JSON encoding/decoding, and optional dependency loading via `MissingDependencyError`.
- [Python Hot-Path and Async Dispatch](../../knowledge/patterns/python/hot-path-and-async-dispatch.md) — Zero-copy `memoryview` slicing for PostgreSQL binary `JSONB` (`\x01` header strip) and `COPY ... TO STDOUT (FORMAT BINARY)` Arrow record batch streaming.

### Requirements

| ID | Requirement |
| --- | --- |
| C1-R1 | Add `postgres` optional extra (`psycopg[binary,pool]>=3.2`, `asyncpg>=0.30`, `pyarrow>=18.0`) in `pyproject.toml` and implement `SyncPostgresDriver` and `AsyncPostgresDriver` in `src/py/vertebra/db/postgres.py` with connection pooling, session GUC application, prepared statement cache sizing, transaction lifecycle (`commit`, `rollback`, `close`), and `stream_arrow_batches` via `COPY (...) TO STDOUT (FORMAT BINARY)`. |
| C1-R2 | Fix `CloudSqlConfig.refresh_lead_seconds` and implement ephemeral X.509 certificate exchange (`EphemeralCertBundle`), ADC IAM token refresh, TLS 1.3 `SSLContext` construction with `0o600` client PEM loading, AlloyDB `MetadataExchangeResponse` decoding, and `connect_sync` / `connect_async` mTLS socket factories in `src/py/vertebra/db/dialer/{cloudsql,alloydb}.py`. |
| C1-R3 | Implement `PostgresVtbDialect` in `src/py/vertebra/extensions/sqlalchemy/dialects.py`, wire `DBAPIConnection` to `SyncPostgresDriver`, populate `DBAPICursor.description` and `lastrowid`, integrate `Vector` (`pgvector` wire syntax), `BinaryJson` (`JSONB`), `InsertSentinelSpec` (`RETURNING`), and `DeleteUsingClause` (`DELETE ... USING`), and align `pyproject.toml` entry points. |
| C1-R4 | Wire `DatabaseRegistry` in `src/py/vertebra/db/_registry.py` for sync and async PostgreSQL and dialer resolution, and add unit and integration acceptance tests in `src/py/tests/unit/db/test_postgres_and_dialers.py`. |

### Target Layout

```text
pyproject.toml                                          add vertebra[postgres] extra and align sqlalchemy.dialects entry points
src/py/vertebra/db/
  _registry.py                                          add get_sync_driver and dialer-attached Postgres driver construction
  postgres.py                                           SyncPostgresDriver (psycopg_pool) + AsyncPostgresDriver (asyncpg/psycopg_pool) + Arrow COPY streaming
  dialer/
    __init__.py                                         export EphemeralCertBundle alongside CloudSql* and AlloyDb*
    cloudsql.py                                         CloudSqlDialer IAM refresh, generateEphemeralCert, TLS 1.3 client PEM context, connect_sync/connect_async
    alloydb.py                                          AlloyDbDialer generateClientCertificate, MetadataExchange request/response, connect_sync/connect_async
src/py/vertebra/extensions/sqlalchemy/
  dialects.py                                           PostgresVtbDialect, PostgreSQL-backed DBAPIConnection, Vector/BinaryJson type map, RETURNING & DELETE USING compilers
src/py/tests/unit/
  data/
    test_postgres_driver.py                             focused unit and acceptance tests for SyncPostgresDriver, AsyncPostgresDriver, and Arrow COPY streaming
    test_cloud_dialers.py                               focused unit tests for CloudSqlDialer, AlloyDbDialer, and mTLS certificate exchange
  extensions/
    test_sqlalchemy.py                                  focused unit tests for PostgresVtbDialect and PostgreSQL DBAPIConnection
```

### Risks

| Risk | Mitigation |
| --- | --- |
| `psycopg` or `asyncpg` is not installed in minimal unit test environments. | `SyncPostgresDriver` and `AsyncPostgresDriver` support `memory://` DSNs and injectable connection/pool factories for unit testing, and raise `MissingDependencyError("psycopg", extra="postgres")` when a live `postgresql://` connection is opened without the extra installed. |
| Google Cloud IAM token strings in tests trigger `git-secrets`. | All tests use `"mock-oauth-token"` instead of `ya29.*` prefixes. |
| Ephemeral private keys written to disk for `SSLContext.load_cert_chain` could leak if world-readable. | `build_tls_context` writes temporary PEM files inside a `0o700` `tempfile.TemporaryDirectory` with `0o600` file permissions and deletes them immediately after `ctx.load_cert_chain(...)` completes. |

## Implementation Plan

### Phase 1: PostgreSQL Drivers, Cloud Dialers, and SQLAlchemy Dialect
- [x] 1.1 Pooled `SyncPostgresDriver`, `AsyncPostgresDriver`, and Arrow binary `COPY` streaming ([worksheet](tasks/1.1.md))
- [x] 1.2 Live `CloudSqlDialer` and `AlloyDbDialer` with IAM refresh, ephemeral X.509 cert exchange, and mTLS sockets ([worksheet](tasks/1.2.md))
- [x] 1.3 SQLAlchemy 2.1 `PostgresVtbDialect` (`postgresql+vtb`) with `Vector`, `BinaryJson`, `RETURNING` sentinel, and `DELETE ... USING` ([worksheet](tasks/1.3.md))
- [x] 1.4 Integration and acceptance tests for PostgreSQL drivers, Cloud SQL/AlloyDB dialers, and `postgresql+vtb` dialect ([worksheet](tasks/1.4.md))
- [x] Chapter gate: `make lint-all && make py-test`

## Requirement-to-task trace

| Requirement | Tasks | Verification |
| --- | --- | --- |
| C1-R1 | 1.1 | `uv run pytest src/py/tests/unit/data/test_postgres_driver.py -q` |
| C1-R2 | 1.2 | `uv run pytest src/py/tests/unit/data/test_cloud_dialers.py -q` |
| C1-R3 | 1.3 | `uv run pytest src/py/tests/unit/extensions/test_sqlalchemy.py -k postgres -q` |
| C1-R4 | 1.4 | `uv run pytest src/py/tests/unit/data/test_postgres_driver.py src/py/tests/unit/data/test_cloud_dialers.py src/py/tests/unit/extensions/test_sqlalchemy.py -q` |

## Finding-to-task trace

| Finding | Task |
| --- | --- |
| FW-01 (PostgreSQL driver) | 1.1 |
| FW-55 | 1.1 |
| FW-04 | 1.2 |
| FW-54 | 1.2 |
| FW-44 (PostgreSQL `+vtb` dialect) | 1.3 |

## Continuity Snapshot

- Active flow: `db-postgres-and-cloud-dialers`, state `planned`. Current task: none. Next step: `/flow:implement db-postgres-and-cloud-dialers`.
- Plan identity: `plan_revision: 2`, `plan_commit: ""`. State identity: `state_revision: 0`.
- Blockers: none.
- Knowledge paths: `.agents/bundles/product/product-guidelines.md`, `.agents/bundles/knowledge/workflow.md`.
