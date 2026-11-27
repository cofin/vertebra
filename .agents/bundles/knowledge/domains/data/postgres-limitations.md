---
type: Gotcha
title: PostgreSQL Native Driver Limitations
description: Operational boundaries for native PostgreSQL pooler modes, logical replication streams and publications, queue executors, Arrow conversion, and SQLSpec protocol alignment.
status: stable
tags: [postgres, limitations, driver, replication]
generated:
  by: flow-implement
  at: "2026-11-12T02:22:54Z"
verified:
  - by: agent:bb-executor
    at: "2026-11-21T01:40:00Z"
    commit: 06e99fbf
  - by: agent:flow-executor
    at: "2026-11-17T01:28:00Z"
    commit: 5a833832
  - by: agent:executor
    at: "2026-11-14T01:15:00Z"
    commit: 7116ba13
sources:
  - id: pg-config-py
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: Pooler mode gating, derive_direct_config, and direct_dsn resolution
  - id: pg-supervisor
    resource: src/rs/vertebra-postgres/src/replication/supervisor.rs::ReplicationSupervisor
    title: Multi-generation verified reconnect supervisor and pooler/identity checks
  - id: pg-change-stream
    resource: src/py/vertebra/db/adapters/postgres/change_stream.py::PostgresChangeSource
    title: Durable CAS checkpoint-before-ack change stream coordinator and SqlPostgresCheckpointStore
  - id: pg-queue-py
    resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
    title: Native PostgreSQL queue stores and optional PostgreSQLQueueExecutor integration
  - id: pg-sqlspec-py
    resource: src/py/vertebra/extensions/sqlspec/adapters/postgres/config.py::PostgresSyncConfig
    title: PostgreSQL SQLSpec adapter configs and ParameterStyle.NUMERIC driver
  - id: pg-arrow-py
    resource: src/py/vertebra/db/arrow.py::ArrowCursor
    title: Zero-copy VTBARROW cursor, Arrow C Stream export, and optional pyarrow/polars conversion
  - id: pg-session-rs
    resource: src/rs/vertebra-postgres/src/session.rs::PostgresConnection::reset_for_pool_return
    title: CockroachDB RESET ALL / cockroach_restart and AlloyDB v1beta/v1alpha fallback contracts
---

# PostgreSQL Native Driver Limitations

<!-- truth: start -->
- **Queue Backend & Stores**: `PostgreSQLQueueBackend` uses native queue stores (`vertebra.db.adapters.postgres.queue`) or an injected `executor=`; without either or a driver/store, it probes `vertebra._postgres` and raises `ImproperConfigurationError` if no non-placeholder DSN is configured [^pg-queue-py].
- **External Poolers (`PgBouncer`, `PgCat`, `pgdog`)**: `PoolerMode::Statement` disables statement caching (`effective_capacity = 0`), `Transaction`/`Auto` disables named prepared statements unless `protocol_level_pooler_cache=True`, and `ReplicationSupervisor` rejects non-session poolers before dialing (`LISTEN/NOTIFY`, migrations, and replication require `direct_dsn` / `derive_direct_config()`) [^pg-config-py] [^pg-supervisor].
- **Logical Replication (`pgoutput` v1)**: Neither `ReplicationSupervisor` nor `PostgresChangeSource` creates or drops slots or publications. Both fail closed on identity mismatch, WAL gap, or CAS conflict (pending commits cap at 1,024; queues `1..=1024` / 8 MiB). `SqlPostgresCheckpointStore` persists checkpoints in a caller-managed table [^pg-supervisor] [^pg-change-stream].
- **Arrow Conversion & Validation**: `ArrowCursor` exports `__arrow_c_schema__`, lazy `__arrow_c_stream__` (no casting on `requested_schema`), `buffer_view()`, and Rust `to_pylist()` (unsupported on union/REE arrays) without `pyarrow`. `to_pyarrow()` / `from_rows()` / `write_arrow_shm()` require `[arrow]`, and `to_polars()` requires `[polars]`. Pre-decode buffer-range validation scales with `batches * buffers` and skips element-width checks for unsupported compressed batches [^pg-arrow-py].
- **Broker Leases & Rewriting**: PostgreSQL drivers take one broker lease at pool checkout and return it on release. Named (`%(name)s`) and `pyformat` (`%s`) bind rewriting runs in Rust (`vertebra._postgres.compile_binds`).
- **SQLSpec & Mock Verification**: `PostgresSyncConfig` / `PostgresAsyncConfig` execute via `ParameterStyle.NUMERIC` (`adapter_name = "vertebra_postgres"`) [^pg-sqlspec-py]. CockroachDB wire paths, AlloyDB `v1beta`/`v1alpha` fallback, extended queue stores, and SQLSpec cursor dispatch are verified against fakes or loopback mocks in unit runs; Docker-backed suites (`test_postgres_command_deadlines.py`, `test_postgres_replication_live.py`) require a running Docker daemon [^pg-session-rs].
<!-- truth: end -->

## Operational Boundaries

### Poolers, Autocommit Reset & Idle Probes

- **Session-Pinned Features Behind Poolers**: Set `direct_dsn` on [`PostgreSQLConfig`](../../../../../src/py/vertebra/db/adapters/postgres/config.py)[^pg-config-py] behind PgBouncer `transaction`/`statement` pooling so `derive_direct_config()` isolates `LISTEN/NOTIFY`, logical replication, and migrations onto direct session connections.
- **Autocommit Pool Reset Bypass**: Outside a transaction or tenant scope, pooled queries ([`pool.rs`](../../../../../src/rs/vertebra-postgres/src/pool.rs)) skip `DISCARD ALL` and retain prepared statements when the command ends idle with a standard tag (`SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `VALUES`, `SHOW`, `EXPLAIN`) and does not mention `set_config`, `advisory`, `temp`, or `unlogged`. Run session-mutating UDFs inside an explicit transaction or tenant scope so full reset occurs.
- **Idle Liveness Probe Window**: `PostgresPool` hands out the most recently returned connection first and skips the idle-socket probe for connections returned under 1 second ago; a server disconnect in that window surfaces as a query error.
- **Bind Rewriting**: `vertebra._postgres.compile_binds` and [`binds.py`](../../../../../src/py/vertebra/db/adapters/postgres/binds.py) rewrite `%(name)s` and `%s` placeholders to `$1..$N`, rejecting ordinary-string backslash escapes in favor of explicit `E'...'`.

### Cancellation & Lease Lifecycles

- **Cancelling Autocommit Pooled Queries**: Pooled autocommit queries are not tracked as the driver's active client, so `cancel()` or `asyncio` cancellation does not send a protocol-level cancel; dropping the future drains and resets the lease within `reset_timeout`. Use `timeout=` or an explicit transaction when protocol cancellation is required.
- **Direct Async Cancellation Retires Sockets**: Cancelling an in-flight `AsyncPostgresCursor` or `AsyncPostgresClient` single-call task invokes `SessionHandle::trigger_async_cancel` and clears cursor state. If cancelled before wire submission, the client remains usable; once a wire cancel is sent, `SessionHandle` retires the socket and subsequent calls raise `InterfaceError("session is not connected")`.
- **Checkout Leases & `execute_many`**: Drivers take the broker lease at `_acquire_pool_client` and release it on `_release_pooled_client`, `_evict_pinned_connections`, or `close()`. `driver.cursor()` takes no lease and rejects `lazy_begin=True` without a pinned transaction or tenant session. `execute_many()` drops `timeout` and `parameter_oids` kwargs because native `executemany` does not accept them per batch.

### Fast Path, Tokens & Testing Limits

- **Async Single-Call Pool Fast Path**: `AsyncPostgreSQLDriver` routes direct query/DML methods through `_single_call()` -> native `fetch_rows`/`fetch_dicts`/`fetch_value`/`execute_dml`, returning the native awaitable directly on pinned, tenant, and `_fast_pool` paths when no token provider, client factory, or broker is active.
- **OAuth2 Clock Source**: `GcpTokenCache` / `ScopedTokenProvider` checks freshness against `SystemTime::now()` and integer `expires_in` seconds with no Python clock-injection hook; tests bound lifetimes via `refresh_skew_ms` and loop `call_later`.
- **SQLSpec & Unit Test Doubles**: `PostgresSyncDriver` / `PostgresAsyncDriver` dispatch via native cursors with `TypedParameter` coercion registered in `build_postgres_statement_config()`. Unit tests (`test_postgres_native_adapters.py`, `test_postgres_compiler_transport.py`) verify these paths against `FakeSyncPostgresPool` / `FakeAsyncPostgresPool`; live benchmark capture requires a reachable Docker daemon.

[^pg-config-py]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — Pooler mode gating, `derive_direct_config`, and `direct_dsn` resolution.
[^pg-supervisor]: `src/rs/vertebra-postgres/src/replication/supervisor.rs::ReplicationSupervisor` — Multi-generation verified reconnect supervisor and pooler/identity checks.
[^pg-change-stream]: `src/py/vertebra/db/adapters/postgres/change_stream.py::PostgresChangeSource` — Durable CAS checkpoint-before-ack change stream coordinator and `SqlPostgresCheckpointStore`.
[^pg-queue-py]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — Native PostgreSQL queue stores and optional `PostgreSQLQueueExecutor` integration.
[^pg-sqlspec-py]: `src/py/vertebra/extensions/sqlspec/adapters/postgres/config.py::PostgresSyncConfig` — PostgreSQL SQLSpec adapter configs and `ParameterStyle.NUMERIC` driver.
[^pg-arrow-py]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Zero-copy `VTBARROW` cursor, Arrow C Stream export, and optional `pyarrow`/`polars` conversion.
[^pg-session-rs]: `src/rs/vertebra-postgres/src/session.rs::PostgresConnection::reset_for_pool_return` — CockroachDB `RESET ALL` / `cockroach_restart` and AlloyDB `v1beta`/`v1alpha` fallback contracts.
