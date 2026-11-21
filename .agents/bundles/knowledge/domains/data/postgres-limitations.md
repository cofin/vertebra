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
- `PostgreSQLQueueBackend` (`vertebra.framework.queues.postgres`) supports native `SyncPostgresQueueStore` / `AsyncPostgresQueueStore` (`vertebra.db.adapters.postgres.queue`) alongside an optional caller-supplied `executor=`; omitting `executor=`, `store=`/`sync_store=`, and `driver=`/`sync_driver=` probes `vertebra._postgres` via `ensure_module(..., extra="postgres")` (`MissingDependencyError`) and raises `ImproperConfigurationError` when no non-empty PostgreSQL DSN (beyond placeholder `"postgres://"` / `"postgresql://"`) is configured [^pg-queue-py].
- External pooler rules (`PgBouncer`, `PgCat`, `pgdog`): `PoolerCapabilities::resolve` sets `effective_capacity = 0` unconditionally under `PoolerMode::Statement`, disables named prepared statement caching under `Transaction`/`Auto` unless `protocol_level_pooler_cache=True` (when `advertised_max_prepared` is `None`), and `ReplicationSupervisor` rejects `PoolerMode` `transaction`, `statement`, or `auto` before dialing (`LISTEN/NOTIFY`, migrations, and logical replication require a session-mode `direct_dsn` / `derive_direct_config()`) [^pg-config-py] [^pg-supervisor].
- `pgoutput` v1 replication does not implicitly create or drop PostgreSQL logical replication streams or publications (`ReplicationSupervisor` / `PostgresChangeSource` fail closed on identity mismatch, WAL gap, or CAS conflict; pending published commits cap at `1024`, queues `1..=1024` / `8 MiB`), and `SqlPostgresCheckpointStore` persists CAS checkpoints in a caller-managed PostgreSQL table [^pg-supervisor] [^pg-change-stream].
- `ArrowCursor.buffer_view()` exports zero-copy `VTBARROW` or offset-0 IPC memory, and both `vertebra._vertebra.ArrowCursor` (`from_ipc_bytes` and SHM backings) and `vertebra.db.arrow.ArrowCursor` / `read_arrow_shm()` export `__arrow_c_schema__()` (`arrow_schema`), lazy `StreamReader`-backed `__arrow_c_stream__()` (`arrow_array_stream`, raising `NotImplementedError` if `requested_schema` requires casting), `columns`, `len()`, `to_polars()`, and pure-Rust `to_pylist()` without `pyarrow`; `to_arrow_table()` / `to_pyarrow()`, `ArrowCursor.from_rows()`, and `write_arrow_shm()` require `[arrow]` (`pyarrow>=18.0.0`), `to_polars()` requires `[polars]` (`polars>=1.0.0`, unpacking Polars 2.x struct-typed `Series` results from `polars.from_arrow` via `.struct.unnest()`), pure-Rust `to_pylist()` raises `NotImplementedError` on Arrow union or run-end-encoded arrays, and live `datafusion` capsule import remains unverified in base environments without the optional `datafusion` wheel [^pg-arrow-py].
- CockroachDB `--cluster=<id>` routing, `crdb_version` `RESET ALL` pool resets, and `cockroach_restart` savepoint retries live on Rust `PostgresConnection` and `SessionHandle`. Those wire paths, AlloyDB `v1beta` to `v1alpha` (HTTP 404 or 501) fallback discovery, and `MetadataExchangeResponse` checks were verified against loopback mocks instead of live CockroachDB or Google Cloud AlloyDB instances [^pg-session-rs].
- `PostgresSyncConfig` and `PostgresAsyncConfig` (`vertebra.extensions.sqlspec.adapters.postgres`) declare concrete `connection_type`, `driver_type`, and `*_store_class` attributes (`adapter_name = "vertebra_postgres"`) for native `ParameterStyle.NUMERIC` execution [^pg-sqlspec-py].
- `SyncPostgresQueueStore` and `AsyncPostgresQueueStore` extended capabilities (`cleanup_terminal`, `reserve_identity`, `get_reservation`, `reset_identity`, `assign_worker`, `touch_heartbeats`, `null_heartbeats`, `interrupt_task`, `expire_overdue`, `get_entry_by_key`, and cross-queue `list_entries(queue=None)`) are verified against fake driver SQL/parameter recordings in unit tests; live PostgreSQL server DDL/query execution remains unverified in unit runs [^pg-queue-py].
- `GcpTokenCache` / `ScopedTokenProvider` in `vertebra-postgres` evaluates OAuth2 token freshness against `SystemClock` (`std::time::SystemTime::now()`) and unsigned integer `expires_in` seconds with no Python clock-injection seam, so OAuth2 refresh tests bound token lifetimes via `refresh_skew_ms` and loop `call_later` waits; live Docker-backed PostgreSQL integration suites (`test_postgres_command_deadlines.py`, `test_postgres_replication_live.py`) require a running Docker daemon and were verified only via Ruff/Mypy when the Docker socket is unavailable.
- `ArrowCursor` checks Arrow IPC buffer ranges and layouts from stream metadata before decoding, so corrupt streams raise `ValueError`. The check scales with batches times buffers, adding open-time cost on streams of many small batches. Element-width checks are skipped for compressed batches, which this build cannot decode [^pg-arrow-py].
- `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` take the broker lease when a connection is checked out of the pool and return it at release, so a worker holds the lease for as long as it holds the connection; named-parameter rewriting works through the native driver and is performed in Rust.
<!-- truth: end -->

## Operational Boundaries

- **Queue Store & Executor Ownership**: [`PostgreSQLQueueBackend`](../../../../../src/py/vertebra/framework/queues/postgres.py)[^pg-queue-py] executes `FOR UPDATE SKIP LOCKED` and `LISTEN/NOTIFY` queries through native `SyncPostgresQueueStore` / `AsyncPostgresQueueStore` (`vertebra.db.adapters.postgres.queue`) when given drivers, stores, or a valid PostgreSQL DSN, or delegates to an injected `PostgreSQLQueueExecutor` when supplied.
- **Session-Pinned Features Behind Poolers**: Set `direct_dsn` on [`PostgreSQLConfig`](../../../../../src/py/vertebra/db/adapters/postgres/config.py)[^pg-config-py] when running behind PgBouncer `transaction` or `statement` pooling so `derive_direct_config()` isolates `LISTEN/NOTIFY`, logical replication (`ReplicationSupervisor`), and DDL/migrations onto direct session connections.
- **SQL Compilation & Named-Parameter Rewriting**: Named-parameter (`%(name)s`) and `pyformat` (`%s`) rewriting works through the native driver and is performed in Rust (`vertebra._postgres.compile_binds`); [`binds.py`](../../../../../src/py/vertebra/db/adapters/postgres/binds.py) normalizes Python parameters to immutable `Pg*`/`bytes` values, and ordinary-string backslash escapes are rejected in favor of explicit `E'...'` or `$1..$N` numeric placeholders.
- **Autocommit Queries Skip Pool Reset**: Outside a transaction or tenant scope, `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` run each query through `SyncPostgresPool.query` / `AsyncPostgresPool.query` ([`pool.rs`](../../../../../src/rs/vertebra-postgres/src/pool.rs)). The connection goes back to the idle queue without `DISCARD ALL` and keeps its prepared statements when the query ends idle with a `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `VALUES`, `SHOW`, or `EXPLAIN` tag and its SQL text does not mention `set_config`, `advisory`, `temp`, or `unlogged`. Any other statement takes the full reset. A user-defined function that changes session state inside one of those statements is not detected, so run such calls inside a transaction or tenant scope.
- **Cancelling Autocommit Queries**: Queries on the pooled path are not tracked as the driver's active client. `cancel()` from another thread and `asyncio` cancellation do not send a protocol-level cancel for them; the lease is drained and reset when the future is dropped, bounded by `reset_timeout`. Use a `timeout=` argument or a transaction when a protocol cancel is required.
- **Idle Liveness Probe Window**: `PostgresPool` hands out the most recently returned connection first. It skips the idle-socket probe for connections returned less than one second earlier, so a server-side disconnect inside that window surfaces as a query error instead of a silent replacement.
- **Async Cursor & Single-Call Cancellation Retires Direct Sessions**: Cancelling an awaiting `AsyncPostgresCursor.execute()`, `executemany()`, or `AsyncPostgresClient.{fetch_rows,fetch_dicts,fetch_value,execute_dml}()` task invokes `SessionHandle::trigger_async_cancel`; cursor calls clear cursor state (`description = None`, `rowcount = -1`) and release the cursor execution flag. When cancellation occurs before wire submission (for example while a single-call statement is queued behind an earlier statement), the direct client remains usable; once a protocol cancel is claimed on an in-flight wire query, `SessionHandle` retires the physical socket and subsequent queries on that same direct `AsyncPostgresClient` raise `InterfaceError("session is not connected")` just like `AsyncPostgresClient.query()`.
- **Driver Checkout Leases and `execute_many` Kwargs**: `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` take the broker lease at pool checkout (`_acquire_pool_client`) and return it at release (`_release_pooled_client`, `_evict_pinned_connections`, and `close()`). A worker holds the lease for as long as it holds the connection. `driver.cursor()` takes no lease and raises `InterfaceError` when `lazy_begin=True` without a pinned transaction or tenant session. `execute_many()` pops `timeout` and `parameter_oids` before annotating SQL because `SyncPostgresCursor.executemany` and `AsyncPostgresCursor.executemany` do not accept per-batch `timeout` or `parameter_oids`. Unit tests in `test_postgres_native_adapters.py` verify these paths against `FakeSyncPostgresPool` / `FakeSyncPostgresCursor` and `FakeAsyncPostgresPool` / `FakeAsyncPostgresCursor` in `postgres_ports.py` rather than a live PostgreSQL server.
- **Async Single-Call Direct Pool Fast Path & Benchmark Capture**: `AsyncPostgreSQLDriver` dispatches `execute`, `execute_dml`, `execute_with_description`, `execute_tuples`, `select`, `select_one`, `select_one_or_none`, and `select_value` through `_single_call()` -> `fetch_rows` / `fetch_dicts` / `fetch_value` / `execute_dml`, returning the native awaitable directly on pinned, tenant, and `_fast_pool` routes (bypassing `_ensure_pool_async` and `_single_call_checked` when no token provider, client factory, or broker is active). Unit tests in `test_postgres_native_adapters.py` verify single-await dispatch, zero helper coroutines, token refresh retry, and leased-route cancellation eviction against `FakeAsyncPostgresPool` / `_CountingAsyncPool`; live PostgreSQL benchmark capture via `tools.benchmarks.adapters.sql capture --engine postgres` requires a reachable Docker daemon (`unix:///var/run/docker.sock`) and was not measured in sandbox runs without Docker.
- **SQLSpec Native Cursor Dispatch & Unit Verification**: `PostgresSyncDriver` and `PostgresAsyncDriver` dispatch through native cursors with `TypedParameter` unwrapping registered in `build_postgres_statement_config().parameter_config.type_coercion_map`; unit tests in `test_postgres_compiler_transport.py` verify single-`execute` dispatch, `execute_many`, `TypedParameter` round-trip, `UniqueViolationError` mapping, and DB-API native cursors against `FakeSyncPostgresPool` / `FakeAsyncPostgresPool` rather than a live PostgreSQL server.

[^pg-config-py]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — Pooler mode gating, `derive_direct_config`, and `direct_dsn` resolution.
[^pg-supervisor]: `src/rs/vertebra-postgres/src/replication/supervisor.rs::ReplicationSupervisor` — Multi-generation verified reconnect supervisor and pooler/identity checks.
[^pg-change-stream]: `src/py/vertebra/db/adapters/postgres/change_stream.py::PostgresChangeSource` — Durable CAS checkpoint-before-ack change stream coordinator and `SqlPostgresCheckpointStore`.
[^pg-queue-py]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — Native PostgreSQL queue stores and optional `PostgreSQLQueueExecutor` integration.
[^pg-sqlspec-py]: `src/py/vertebra/extensions/sqlspec/adapters/postgres/config.py::PostgresSyncConfig` — PostgreSQL SQLSpec adapter configs and `ParameterStyle.NUMERIC` driver.
[^pg-arrow-py]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Zero-copy `VTBARROW` cursor, Arrow C Stream export, and optional `pyarrow`/`polars` conversion.
[^pg-session-rs]: `src/rs/vertebra-postgres/src/session.rs::PostgresConnection::reset_for_pool_return` — CockroachDB `RESET ALL` / `cockroach_restart` and AlloyDB `v1beta`/`v1alpha` fallback contracts.
