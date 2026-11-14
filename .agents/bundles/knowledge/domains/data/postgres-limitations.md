---
type: Gotcha
title: PostgreSQL Native Driver Limitations
description: Operational boundaries for native PostgreSQL pooler modes, logical replication slots/publications, queue executors, Arrow conversion, and SQLSpec protocol alignment.
status: stable
tags: [postgres, limitations, driver, replication]
generated:
  by: flow-implement
  at: "2026-11-12T02:22:54Z"
verified:
  - by: agent:executor
    at: "2026-11-14T01:15:00Z"
    commit: 8c615b73
sources:
  - id: pg-config-py
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: Pooler mode gating, derive_direct_config, and direct_dsn resolution
  - id: pg-supervisor
    resource: src/rs/vertebra-postgres/src/replication/supervisor.rs::ReplicationSupervisor
    title: Multi-generation verified reconnect supervisor and pooler/identity checks
  - id: pg-change-stream
    resource: src/py/vertebra/db/adapters/postgres/change_stream.py::PostgresChangeSource
    title: Durable CAS checkpoint-before-ack change stream coordinator
  - id: pg-queue-py
    resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
    title: Caller-supplied queue executor and vertebra._postgres dependency guard
  - id: pg-sqlspec-py
    resource: src/py/vertebra/extensions/sqlspec/adapters.py::PostgreSQLSyncConfig
    title: PostgreSQL SQLSpec adapter configs and ParameterStyle.NUMERIC driver
  - id: pg-arrow-py
    resource: src/py/vertebra/db/arrow.py::ArrowCursor
    title: Zero-copy VTBARROW cursor and optional pyarrow/polars conversion
---

# PostgreSQL Native Driver Limitations

<!-- truth: start -->
- `PostgreSQLQueueBackend` (`vertebra.framework.queues.postgres`) requires a caller-supplied `executor=`; omitting `executor=` probes `vertebra._postgres` via `ensure_module(..., extra="postgres")` (`MissingDependencyError`) and raises `ImproperConfigurationError` when present without `executor=` [^pg-queue-py].
- External transaction/statement poolers (`PgBouncer`, `PgCat`, `pgdog`) disable named prepared statement caching unless `protocol_level_pooler_cache=True`, and `ReplicationSupervisor` rejects `PoolerMode` `transaction`, `statement`, or `auto` modes before dialing (`LISTEN/NOTIFY`, migrations, and logical replication require a session-mode `direct_dsn` / `derive_direct_config()`) [^pg-config-py] [^pg-supervisor].
- `pgoutput` v1 replication does not implicitly create or drop PostgreSQL replication slots or publications (`ReplicationSupervisor` / `PostgresChangeSource` fail closed on identity mismatch, WAL gap, or CAS conflict; pending published commits cap at `1024`, queues `1..=1024` / `8 MiB`) [^pg-supervisor] [^pg-change-stream].
- `ArrowCursor.buffer_view()` exports zero-copy `VTBARROW` IPC memory with no optional Arrow packages, whereas `to_arrow_table()` / `load_from_arrow` stream conversion and `to_polars()` require `[arrow]` (`pyarrow>=18.0.0`) or `[polars]` (`polars>=1.0.0`); direct Rust C Data Interface `FFI_ArrowArrayStream` export is tracked in GitHub issue `#128` [^pg-arrow-py].
- `PostgreSQLSyncConfig` and `PostgreSQLAsyncConfig` declare concrete `connection_type` and `driver_type` attributes for native `ParameterStyle.NUMERIC` execution; upstream `sqlspec` `DatabaseConfigProtocol` structural typing alignment is tracked in GitHub issue `#131` [^pg-sqlspec-py].
<!-- truth: end -->

## Operational Boundaries

- **Queue Executor Ownership**: [`PostgreSQLQueueBackend`](../../../../../src/py/vertebra/framework/queues/postgres.py)[^pg-queue-py] executes `FOR UPDATE SKIP LOCKED` and `LISTEN/NOTIFY` queries through an injected `PostgreSQLQueueExecutor` until dedicated native queue stores ship under the `queues-tasks-and-channels` specification.
- **Session-Pinned Features Behind Poolers**: Set `direct_dsn` on [`PostgreSQLConfig`](../../../../../src/py/vertebra/db/adapters/postgres/config.py)[^pg-config-py] when running behind PgBouncer `transaction` or `statement` pooling so `derive_direct_config()` isolates `LISTEN/NOTIFY`, logical replication (`ReplicationSupervisor`), and DDL/migrations onto direct session connections.
- **SQL Compilation & Pyformat Emulation**: Native drivers normalize Python parameters to immutable `Pg*`/`bytes` values; `pyformat` emulation in [`binds.py`](../../../../../src/py/vertebra/db/adapters/postgres/binds.py) rejects ambiguous ordinary-string backslash escapes in favor of explicit `E'...'` or `$1..$N` numeric placeholders.

[^pg-config-py]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — Pooler mode gating, `derive_direct_config`, and `direct_dsn` resolution.
[^pg-supervisor]: `src/rs/vertebra-postgres/src/replication/supervisor.rs::ReplicationSupervisor` — Multi-generation verified reconnect supervisor and pooler/identity checks.
[^pg-change-stream]: `src/py/vertebra/db/adapters/postgres/change_stream.py::PostgresChangeSource` — Durable CAS checkpoint-before-ack change stream coordinator.
[^pg-queue-py]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — Caller-supplied queue executor and `vertebra._postgres` dependency guard.
[^pg-sqlspec-py]: `src/py/vertebra/extensions/sqlspec/adapters.py::PostgreSQLSyncConfig` — PostgreSQL SQLSpec adapter configs and `ParameterStyle.NUMERIC` driver.
[^pg-arrow-py]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Zero-copy `VTBARROW` cursor and optional `pyarrow`/`polars` conversion.
