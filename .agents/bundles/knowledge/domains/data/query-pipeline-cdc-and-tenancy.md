---
type: Guide
title: "Native Pipelines, Change Feeds, Arrow IPC, Tenant Predicates & DataFusion"
description: "Unified pipeline exit contracts, fail-loud Arrow IPC and Arrow C Stream export, engine-neutral change feeds, SQL-backed PostgreSQL CDC checkpoints, Oracle/PostgreSQL notifications, VPD/RLS tenant scopes, and DataFusion."
status: stable
tags:
  - domains
  - database
  - postgres
  - oracle
  - arrow
  - cdc
  - tenancy
generated:
  by: flow/0.30.1
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-17T01:22:00Z"
    commit: 5a833832
  - by: agent:flow-completion
    at: "2026-11-14T21:38:00Z"
    commit: c526639f
sources:
  - id: src-postgres-stream-rs
    resource: src/rs/vertebra-postgres/src/replication/stream.rs::ReplicationSession
    title: "Bounded replication session, publication leases and shutdown"
  - id: src-postgres-pgoutput-rs
    resource: src/rs/vertebra-postgres/src/replication/pgoutput.rs::PgOutputDecoder
    title: "Incremental pgoutput v1 transaction records and bounded queue"
  - id: src-postgres-tenant-core
    resource: src/py/vertebra/db/adapters/postgres/core.py
    title: "PostgreSQL tenant and session SQL builders"
  - id: src-postgres-pipeline-py
    resource: src/py/vertebra/db/adapters/postgres/pipeline.py
    title: "SyncPostgresPipeline and AsyncPostgresPipeline unified exit contract"
  - id: src-sqlite-pipeline-py
    resource: src/py/vertebra/db/adapters/sqlite/pipeline.py
    title: "SyncSQLitePipeline and AsyncSQLitePipeline unified exit contract"
  - id: src-mysql-pipeline-py
    resource: src/py/vertebra/db/adapters/mysql/pipeline.py
    title: "SyncMySQLPipeline and AsyncMySQLPipeline unified exit contract"
  - id: src-mssql-pipeline-py
    resource: src/py/vertebra/db/adapters/mssql/pipeline.py
    title: "SyncMSSQLPipeline and AsyncMSSQLPipeline unified exit contract"
  - id: src-oracle-tenant-core
    resource: src/py/vertebra/db/adapters/oracle/core.py
    title: "Oracle VPD trusted-package and SET_IDENTIFIER context SQL builders"
  - id: src-oracle-pipeline-py
    resource: src/py/vertebra/db/adapters/oracle/pipeline.py
    title: "SyncOraclePipeline and AsyncOraclePipeline delayed-commit batching"
  - id: src-oracle-driver-py
    resource: src/py/vertebra/db/adapters/oracle/driver.py
    title: "SyncOracleDriver and AsyncOracleDriver VPD reset-or-evict pool hooks"
  - id: src-db-oracle-notify-py
    resource: src/py/vertebra/db/adapters/oracle/notify.py
    title: "OracleNotifyConfig and Sync/AsyncOracleNotificationSource TxEventQ / DBMS_AQ streaming"
  - id: src-vertebra-oracle-notify-rs
    resource: src/rs/vertebra-oracle/src/notify.rs
    title: "Dedicated AsyncOracleListener DBMS_AQ.DEQUEUE worker pump"
  - id: src-extensions-apex-session-py
    resource: src/py/vertebra/extensions/apex/session.py
    title: "OracleSessionManager APEX VPD propagation and ECID piggyback"
  - id: src-extensions-sqlspec-bigquery-py
    resource: src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py
    title: "BigQuery SQLSpec native statement defaults and session execution"
  - id: src-extensions-sqlspec-spanner-py
    resource: src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py
    title: "Spanner SQLSpec parser and native parameter defaults"
  - id: src-extensions-sqlspec-adapters-py
    resource: src/py/vertebra/extensions/sqlspec/_driver.py
    title: "SQLSpec native pipeline dispatch and sequential stack execution"
  - id: src-db-duckdb-pipeline-py
    resource: src/py/vertebra/db/adapters/duckdb/pipeline.py
    title: "Delayed-commit SyncDuckDBPipeline and AsyncDuckDBPipeline with savepoint nesting"
  - id: src-db-arrow-py
    resource: src/py/vertebra/db/arrow.py
    title: "Fail-loud ArrowCursor, write_arrow_shm, read_arrow_shm, and Arrow C Stream export"
  - id: src-vertebra-py-db-rs
    resource: src/rs/vertebra-py/src/db.rs
    title: "Native ArrowCursor.__arrow_c_stream__ PyCapsule export"
  - id: src-db-cdc-py
    resource: src/py/vertebra/db/cdc.py
    title: "Engine-neutral ChangeFeed with locked _next_id increments and spill hydration"
  - id: src-db-spanner-change-stream-py
    resource: src/py/vertebra/db/adapters/spanner/change_stream.py
    title: "Spanner change-stream readers and partition record types"
  - id: src-db-postgres-notify-py
    resource: src/py/vertebra/db/adapters/postgres/notify.py
    title: "PostgreSQL notification transport and logical message builders"
  - id: src-extensions-sqlspec-events-py
    resource: src/py/vertebra/extensions/sqlspec/events.py
    title: "Engine-neutral SQLSpec change-feed event backends"
  - id: src-postgres-supervisor-rs
    resource: src/rs/vertebra-postgres/src/replication/supervisor.rs::ReplicationSupervisor
    title: "Generation-fenced reconnect supervision and identity verification"
  - id: src-db-postgres-change-stream-py
    resource: src/py/vertebra/db/adapters/postgres/change_stream.py::PostgresChangeSource
    title: "Durable PostgreSQL CDC source with MemoryPostgresCheckpointStore and SqlPostgresCheckpointStore"
  - id: src-db-rls-py
    resource: src/py/vertebra/db/rls.py
    title: "TenantScopeConfig, TenantPredicate, and reset-or-evict tenant scopes"
  - id: src-db-datafusion-config-py
    resource: src/py/vertebra/db/adapters/datafusion/config.py
    title: "DataFusionConfig and TableRegistration"
  - id: src-db-datafusion-core-py
    resource: src/py/vertebra/db/adapters/datafusion/core.py
    title: "DataFusionCore session builder, source registration, and explain helpers"
  - id: src-db-datafusion-driver-py
    resource: src/py/vertebra/db/adapters/datafusion/driver.py
    title: "DataFusionEngine federated Arrow SessionContext"
  - id: src-db-registry-py
    resource: src/py/vertebra/db/registry.py
    title: "Polymorphic DatabaseRegistry URL and config resolution"
  - id: src-db-driver-capabilities-py
    resource: src/py/vertebra/db/driver/capabilities.py
    title: "Sync/Async pipeline and change-source capability protocols"
---

# Native Pipelines, Change Feeds, Arrow IPC, Tenant Predicates & DataFusion

<!-- truth: start -->
- **Unified Pipeline Exit & Transaction Ownership Contract (`pipeline.py`)**: All six relational pipeline pairs (`SQLite`, `DuckDB`, `MySQL`, `MSSQL`, `Oracle`, `PostgreSQL`) share one exit contract: clean `__exit__`/`__aexit__` flushes queued operations and commits only a transaction the pipeline itself opened (or releases its nested `_vtb_*` savepoint); exit with an exception discards queued operations and rolls back only a pipeline-owned transaction or savepoint; caller-owned transactions are never committed or rolled back by the pipeline [^src-sqlite-pipeline-py] [^src-postgres-pipeline-py] [^src-oracle-pipeline-py].
- **Fail-Loud Arrow IPC & Native Arrow C Stream Export (`db/arrow.py`, `vertebra-py/src/db.rs`)**: `ArrowCursor.to_arrow_table()`, `ArrowCursor.to_polars()`, `write_arrow_shm()`, and `read_arrow_shm()` raise `MissingDependencyError` via `ensure_module` when optional packages are absent and `DataError` when payloads are empty or invalid Arrow IPC (never falling back to `self` or JSON). Native `vertebra._vertebra.ArrowCursor` and Python `ArrowCursor` implement `__arrow_c_stream__(requested_schema=None)` exporting an `arrow_array_stream` `PyCapsule` [^src-db-arrow-py] [^src-vertebra-py-db-rs].
- **Native PostgreSQL Replication & `SqlPostgresCheckpointStore`**: `PgOutputDecoder::next_record` emits ordered v1 transaction envelopes, `ReplicationSession` runs concurrent reader/writer loops (`1–1024` records, `8 MiB` / `16 MiB` bounds, cancel-safe delivery leases), `ReplicationSupervisor` handles generation-fenced reconnects, and [`PostgresChangeSource`](../../../../../src/py/vertebra/db/adapters/postgres/change_stream.py)[^src-db-postgres-change-stream-py] commits CAS checkpoints via `MemoryPostgresCheckpointStore` or SQL-backed `SqlPostgresCheckpointStore` strictly before `acknowledge(lsn)` [^src-postgres-pgoutput-rs] [^src-postgres-stream-rs] [^src-postgres-supervisor-rs].
- **Thread-Safe `ChangeFeed` & Streaming Adapters (`cdc.py`, `notify.py`, `change_stream.py`)**: `ChangeFeed` serializes `_next_id` increments under `self._id_lock` (`threading.Lock`), while `SyncPostgresNotificationSource` / `AsyncPostgresNotificationSource`, `SyncOracleNotificationSource` / `AsyncOracleNotificationSource` (`AsyncOracleListener`), and `SyncSpannerChangeStreamSource` / `AsyncSpannerChangeStreamSource` implement adapter streaming [^src-db-cdc-py] [^src-db-postgres-notify-py] [^src-db-oracle-notify-py] [^src-db-spanner-change-stream-py].
- **Reset-or-Evict Tenant Scoping & Configured DataFusion**: `TenantConnectionScope` / `AsyncTenantConnectionScope` (`vertebra.db.rls`), `OracleCore`, and `OracleSessionManager` always clear session context on exit and evict sessions on failure. `DataFusionEngine` (`mode="in_process"`) builds its `SessionContext` via `DataFusionCore.build_session_context` (`target_partitions` and `memory_limit_mb`), registers Parquet/CSV/NDJSON/`SyncObjectStore`/Arrow sources, and formats planner-backed `explain_plan` output [^src-db-rls-py] [^src-db-datafusion-core-py].
<!-- truth: end -->

The native pipeline capability protocols, Arrow IPC helpers, and tenant scopes in [`vertebra.db`](../../../../../src/py/vertebra/db/__init__.py) define engine batching, zero-copy Arrow transfer, and session isolation contracts. [`cdc.py`](../../../../../src/py/vertebra/db/cdc.py)[^src-db-cdc-py] manages engine-neutral change-feed envelopes, [`postgres.notify`](../../../../../src/py/vertebra/db/adapters/postgres/notify.py)[^src-db-postgres-notify-py] owns PostgreSQL notification SQL, [`oracle.notify`](../../../../../src/py/vertebra/db/adapters/oracle/notify.py)[^src-db-oracle-notify-py] and [`vertebra-oracle/src/notify.rs`](../../../../../src/rs/vertebra-oracle/src/notify.rs)[^src-vertebra-oracle-notify-rs] own Oracle 26ai TxEventQ / `DBMS_AQ` streaming, [`spanner.change_stream`](../../../../../src/py/vertebra/db/adapters/spanner/change_stream.py)[^src-db-spanner-change-stream-py] owns Spanner change streams, and [`vertebra.db.adapters.datafusion`](../../../../../src/py/vertebra/db/adapters/datafusion/__init__.py) maintains federated analytical sessions. All SQL builders validate identifiers against `^[A-Za-z_][A-Za-z0-9_]*$` (`validate_identifier`, or AQ's `^[A-Za-z_][A-Za-z0-9_$#]{0,127}$`). Drivers and dialers live in [database-drivers-and-dialers](database-drivers-and-dialers.md).

## 1. Native Pipeline Capabilities & Fail-Loud Arrow IPC

[`SyncPipelineCapable` / `AsyncPipelineCapable`](../../../../../src/py/vertebra/db/driver/capabilities.py)[^src-db-driver-capabilities-py] expose `driver.pipeline()` as a sync or async context manager (`pipe.select(sql, params)`, `pipe.execute(sql, params)`, `pipe.results()`). All six relational pipeline implementations share one exit and transaction-ownership contract:

- **SQLite, DuckDB, MySQL, MSSQL, Oracle, and PostgreSQL (`pipeline.py`)**: Entering a pipeline outside an active transaction begins a pipeline-owned transaction (`_owns_tx = True`); entering inside an existing caller transaction creates a nested savepoint (`_owns_tx = False`). On a clean `__exit__`/`__aexit__` (`exc_type is None` and not already failed), remaining queued operations flush through `results()`, and the pipeline commits only if `_owns_tx` is `True` (or releases its nested savepoint). On an exception exit or flush failure, remaining queued operations are cleared (`_ops.clear()`), and the pipeline rolls back only its own transaction or nested savepoint (`ROLLBACK TO SAVEPOINT`); caller-owned outer transactions are never committed or rolled back by the pipeline [^src-sqlite-pipeline-py] [^src-db-duckdb-pipeline-py] [^src-mysql-pipeline-py] [^src-mssql-pipeline-py] [^src-oracle-pipeline-py] [^src-postgres-pipeline-py].
- **Fail-Loud Arrow IPC & `__arrow_c_stream__` (`db/arrow.py`, `vertebra-py/src/db.rs`)**: [`ArrowCursor.to_arrow_table()`](../../../../../src/py/vertebra/db/arrow.py)[^src-db-arrow-py], `ArrowCursor.to_polars()`, `write_arrow_shm()`, and `read_arrow_shm()` require optional packages via `ensure_module` (`MissingDependencyError`) and raise `DataError` whenever an input buffer is empty or fails Arrow IPC stream decoding (with `ValueError` on malformed `VTBARROW` headers in `read_arrow_shm`); no path falls back to `self` or JSON. Native `vertebra._vertebra.ArrowCursor.__arrow_c_stream__` ([`src/rs/vertebra-py/src/db.rs`](../../../../../src/rs/vertebra-py/src/db.rs)[^src-vertebra-py-db-rs]) and Python `ArrowCursor.__arrow_c_stream__` export an `arrow_array_stream` `PyCapsule` for zero-copy Arrow PyCapsule consumers (rejecting unsupported `requested_schema` casts).
- **SQLSpec Stack Dispatch**: [`BaseSyncDriver.execute_stack` / `BaseAsyncDriver.execute_stack`](../../../../../src/py/vertebra/extensions/sqlspec/_driver.py)[^src-extensions-sqlspec-adapters-py] dispatch multi-statement `execute` stacks with `continue_on_error=False` through `driver.pipeline()`, preserving prepared operation types (including `INSERT ... RETURNING`) in ordered `StackResult` entries. Driver-owned statement defaults preserve native bind styles without importing optional upstream SDKs: PostgreSQL compiles `$1..$N` (`ParameterStyle.NUMERIC`), MySQL uses positional `?` (`ParameterStyle.QMARK`), `OracleSyncDriver` / `OracleAsyncDriver` set `_execution_parameter_style = ParameterStyle.NAMED_COLON`, BigQuery owns `bigquery` named-`@` parameters, and Spanner selects `spanner` named-`@` (GoogleSQL) or `spangres` numeric parameters [^src-extensions-sqlspec-bigquery-py] [^src-extensions-sqlspec-spanner-py].

## 2. Tenant Isolation (`rls.py`)

| Symbol | Responsibility |
|---|---|
| [`TenantScopeConfig` / `TenantPredicate`](../../../../../src/py/vertebra/db/rls.py)[^src-db-rls-py] | Frozen engine-neutral scope (`setting_name="app.tenant_id"`, `tenant_column="tenant_id"`, `parameter_cast=None`) and predicate (`table_alias=""`, `tenant_value`, `column_reference()`, ANSI `to_sql_fragment(placeholder="?")` applying optional `CAST(<placeholder> AS <type>)` to preserve column indexes). |
| [`TenantConnectionScope` / `AsyncTenantConnectionScope`](../../../../../src/py/vertebra/db/rls.py)[^src-db-rls-py] | Sync/async context managers calling `driver.enter_tenant_scope(tenant_value, config)` on entry and guaranteeing `driver.exit_tenant_scope(config)` on exit; if reset raises, invokes `driver.evict_connection()` (falling back to `close()`) before re-raising so tainted sessions never return to a pool. |
| [`PostgreSQLCore`](../../../../../src/py/vertebra/db/adapters/postgres/core.py)[^src-postgres-tenant-core] | `build_session_guc_sql` binds `(setting_name, tenant_value, local)` in `SELECT set_config($1, $2, $3)`; `build_tenant_predicate_sql` renders `$N` with optional `::<type>` cast. |
| [`OracleCore`](../../../../../src/py/vertebra/db/adapters/oracle/core.py)[^src-oracle-tenant-core] | `build_session_context_sql` and `build_clear_context_sql` route `OracleConfig.context_package_proc` / `context_clear_proc` (`validate_identifier(..., allow_dots=True)`) to trusted PL/SQL procedures (`BEGIN <proc>(:namespace, :attribute, :value); END;`) and default `namespace="USERENV"` to `DBMS_SESSION.SET_IDENTIFIER(:value)` / `DBMS_SESSION.CLEAR_IDENTIFIER` so Oracle 26ai never fails with unprivileged `ORA-01031`. |

`BaseSyncDriver.apply_tenant_predicate` and its async counterpart refuse unsupported engines with native `NotSupportedError`. PostgreSQL sync and async adapters both compile session GUC SQL (`SELECT set_config($1, $2, $3)`) through `ParameterStyle.NUMERIC` `$N` binds and unwrap typed SQLSpec parameters before native `vertebra._postgres` transport. Oracle adapters bind the context setter and return a named predicate, propagating original setter exceptions unchanged.

BigQuery and Spanner's eight SQLSpec config/driver constructors reject a non-None `tenant_predicate` as their first executable statement, before base initialization, core access, config resolution, broker allocation, or native side effects.

Because Oracle `SYS_CONTEXT` and `USERENV.CLIENT_IDENTIFIER` survive `ROLLBACK`, [`SyncOracleDriver` / `AsyncOracleDriver`](../../../../../src/py/vertebra/db/adapters/oracle/driver.py)[^src-oracle-driver-py] (`enter_tenant_scope`, `exit_tenant_scope`, `evict_connection`) and [`OracleSessionManager`](../../../../../src/py/vertebra/extensions/apex/session.py)[^src-extensions-apex-session-py] (`build_attach_plsql`, `build_detach_plsql`, `acquire_scoped`, plus TTC `ECID` `set_session_attrs` piggyback) execute explicit clear procedures before pool release and permanently evict the physical session if detach or reset raises. Live Oracle `DBMS_SESSION.SET_CONTEXT` execution requires a trusted package associated with `CREATE CONTEXT` in the call stack, and no helper provisions RLS or VPD policies ([Oracle DBMS_SESSION reference](https://docs.oracle.com/en/database/oracle/oracle-database/26/arpls/DBMS_SESSION.html)).

### Generic Database Engine Purity

The AST gate scans `db/*.py` and `db/driver/*.py` for adapter imports, executable adapter-class use, literal adapter loading, and engine-specific SQL construction. It excludes docstrings, annotations, and engine metadata. The registry permits only module-level adapter config imports directly listed in `REGISTERED_CONFIG_TYPES`, their declarative tuple and annotation/type-alias uses, and guarded `TYPE_CHECKING` driver imports used in annotations/type aliases. `SCHEME_REGISTRY` derives URL schemes from that tuple. Runtime driver imports, engine-class branches, identity dispatch, and direct construction are forbidden.

Cloud SQL and AlloyDB transport dialers belong in `db.dialer` with dedicated registry branches. Polymorphic `config_cls.from_url`, `config.build_sync_driver`, `config.build_async_driver`, and `ensure_module(self.driver_module)` provide configuration-owned driver resolution across stable broker dialect IDs. [^src-db-registry-py]

## 3. Change Feed (`cdc.py`) & `SqlPostgresCheckpointStore`

| Symbol | Responsibility |
|---|---|
| [`ChangeEvent`](../../../../../src/py/vertebra/db/cdc.py)[^src-db-cdc-py] | `FrozenBaseStruct(frozen=True, gc=False, kw_only=True)`: `event_id`, `channel`, `operation`, `payload`, `spilled`. |
| `CDCConfig` | Engine-neutral frozen configuration: `channel="vertebra_events"`, `spill_threshold_bytes=7500`, `spill_prefix="cdc/spill"`, `max_history_size=1024`, `max_memory_spills=1024`. `max_memory_spills` must be at least `1`. |
| `ChangeFeed.publish_event(operation, payload)` | Assigns a monotonically increasing `event_id` guarded by `self._id_lock` (`threading.Lock`) and serializes with `encode_json_bytes`. Payloads strictly larger than the byte threshold spill to an attached `SyncObjectStore` or bounded in-memory `OrderedDict`; the wire envelope carries `spilled: true` and `spill_key` without the payload. Returns `(wire_envelope, ChangeEvent)` and appends to bounded replay history. |
| `ChangeFeed.hydrate_notification(envelope)` | Decodes with `decode_json`, restores inline payloads, or hydrates spills from `SyncObjectStore` or the memory cache. Successful memory hydration refreshes LRU recency. Store errors propagate directly; an evicted or unknown memory spill raises `KeyError("Evicted or unknown in-memory CDC spill id: <id>")`. |
| `ChangeFeed.replay_since(last_event_id)` | Returns buffered events newer than the id. |
| [`PostgresNotifyConfig`](../../../../../src/py/vertebra/db/adapters/postgres/notify.py)[^src-db-postgres-notify-py] | Frozen PostgreSQL notification configuration: `channel="vertebra_events"`, `spill_threshold_bytes=7500`, `spill_table="vertebra_notify_spill"`, `spill_prefix="cdc/spill"`, `replication_slot_name="vertebra_cdc_slot"`, `publication_name="vertebra_pub"`, `max_slot_wal_keep_size_mb=4096`. Validates channel, table, slot, and publication as single SQL identifiers. |
| `SyncPostgresNotificationSource` / `AsyncPostgresNotificationSource` | Implements `SyncNotificationSource` / `AsyncNotificationSource` (`publish`, `subscribe`, `unsubscribe`, `listen`). With a `ChangeFeed`, publishes canonical `PUBLISH` envelopes containing `channel` and base64 `data_b64`; otherwise strings pass through and bytes use a `b64:` prefix. Subscription identifiers are validated. Sync and async drivers expose `listen_notifications(config=None, *, change_feed=None, queue_capacity=1024, reconnect=False)`. |
| `build_emit_message_sql` / `build_notify_sql` (static) | `build_emit_message_sql` returns `("SELECT pg_logical_emit_message($1, $2, $3)", (transactional, prefix, payload_bytes))`; `build_notify_sql` validates the channel and returns `("SELECT pg_notify($1, $2)", (channel, payload))`. |
| [`PostgresChangeSource`](../../../../../src/py/vertebra/db/adapters/postgres/change_stream.py)[^src-db-postgres-change-stream-py] & [`SqlPostgresCheckpointStore`](../../../../../src/py/vertebra/db/adapters/postgres/change_stream.py) | Durable `AsyncChangeSource` over `AsyncPostgresReplication`: runs `PostgresCheckpointStore` (`MemoryPostgresCheckpointStore` or SQL-backed `SqlPostgresCheckpointStore` over `AsyncPostgreSQLDriver`, providing `ddl()`, async-locked `ensure_table()`, `load(identity_key)`, and optimistic versioned CAS `save(checkpoint, *, expected=...)` persisting `identity_key`, `commit_end_lsn`, `next_event_id`, and `version`) strictly before `acknowledge(lsn)`, raising `PostgresReplicationGapError` or `PostgresCheckpointConflictError` fail-closed. |
| [`OracleNotifyConfig` / `SyncOracleNotificationSource` / `AsyncOracleNotificationSource`](../../../../../src/py/vertebra/db/adapters/oracle/notify.py)[^src-db-oracle-notify-py] & [`AsyncOracleListener`](../../../../../src/rs/vertebra-oracle/src/notify.rs)[^src-vertebra-oracle-notify-rs] | Implements `SyncNotificationSource` / `AsyncNotificationSource` (`publish`, `subscribe`, `unsubscribe`, `listen`) plus `subscribe_aq`/`publish_aq`/`listen_aq`/`close`. Validates AQ queue identifiers (`^[A-Za-z_][A-Za-z0-9_$#]{0,127}$`, `ProgrammingError` `42602`), runs 1s-sliced server-blocking `DBMS_AQ.DEQUEUE` / TxEventQ iterations on a dedicated non-pooled Rust worker thread (`1024`-frame oldest-drop buffer), retries transient disconnects with exponential backoff, and hydrates optional `ChangeFeed` envelopes. |
| [`ChangeFeedSyncEventBackend` / `ChangeFeedAsyncEventBackend`](../../../../../src/py/vertebra/extensions/sqlspec/events.py)[^src-extensions-sqlspec-events-py] | Engine-neutral SQLSpec event backends support `publish`, `publish_many`, spill-hydrating `dequeue`, `ack`, `nack` redelivery, and `shutdown`. |
| [`SyncSpannerChangeStreamSource` / `AsyncSpannerChangeStreamSource`](../../../../../src/py/vertebra/db/adapters/spanner/change_stream.py)[^src-db-spanner-change-stream-py] | Implements [`SyncChangeSource` / `AsyncChangeSource`](../../../../../src/py/vertebra/db/driver/capabilities.py)[^src-db-driver-capabilities-py] with zero-argument `stream()` calls, parsing `DataChangeRecord`, `HeartbeatRecord`, and `ChildPartitionsRecord` payloads into `SpannerChangeStreamRecord` and `SpannerChildPartition` structs across GoogleSQL (`READ_<stream_name>`) and Spangres (`spanner.read_json_<stream_name>`), fanning out child partition readers and forwarding data changes into `ChangeFeed.publish_event()`. |

Replay history buffers up to `max_history_size` events. Without an object store, spill payloads use an LRU cache bounded by `max_memory_spills`; publication evicts the least recently used spill when the bound is exceeded. Hydration refreshes recency, and an evicted envelope fails explicitly instead of returning an empty payload.

Store keys use `spill_prefix.rstrip("/")`: a non-empty normalized prefix yields `<prefix>/<event_id>.json`, and an empty prefix yields `<event_id>.json`. Publication and hydration without an explicit `spill_key` apply the same normalization. `SyncObjectStore.get()` exceptions, including missing-object errors, reach the caller unchanged. PostgreSQL replication and notification settings belong to `PostgresNotifyConfig`, while `CDCConfig` holds only engine-neutral fields.

### Native PostgreSQL Session Ownership

`PostgresReplicationConnection::into_stream` initializes feedback from its connection-owned successful START LSN. Received, published, and acknowledged Commit ends are tracked separately; flush/apply never advance from `wal_end`. At most 1024 published Commits await acknowledgement. Reader admission pauses at capacity while periodic status writes continue; only parsed keepalive reply requests can trigger immediate responses. [^src-postgres-stream-rs]

`next()` allows one pending receiver or held delivery lease. Dropping a pending future releases the reservation; dropping a held lease restores the same record before later FIFO records. Explicit `shutdown()` fences publication, clears session-owned records, and uses one absolute deadline for final feedback, frontend CopyDone, backend CopyDone/CommandComplete/ReadyForQuery and socket closure. `cancel()` requests synchronous fencing and worker abortion without requiring a runtime; caller-owned leases keep valid borrows until consumed or dropped. Natural clean CopyDone closes transport while preserving decoded records for draining and publication. `wait_closed()` supports multiple cancellation-safe waiters and preserves the first terminal error. Frame-boundary transport loss is handled by [`ReplicationSupervisor`](../../../../../src/rs/vertebra-postgres/src/replication/supervisor.rs)[^src-postgres-supervisor-rs]; truncated frames and unfinished transactions at normal CopyDone are protocol errors. [^src-postgres-stream-rs]

## 4. DataFusion Federation Adapter (`adapters/datafusion/`)

| Symbol | Responsibility |
|---|---|
| `ARROW_STREAM_MEDIA_TYPE` | `"application/vnd.apache.arrow.stream"`, also exposed as `DataFusionEngine.ARROW_STREAM_MEDIA_TYPE`. |
| `DataFusionMode` | `Literal["in_process"]` (`pgwire_fdw` and `host:port` endpoints raise `ImproperConfigurationError`). |
| [`TableRegistration`](../../../../../src/py/vertebra/db/adapters/datafusion/config.py)[^src-db-datafusion-config-py] | `name`, `uri`, `format` (`parquet`, `vortex`, `arrow`, `csv`, `ndjson`, `duckdb`, `postgres`, `oracle`, `bigquery`, `spanner`). |
| `DataFusionConfig` | `BaseDatabaseConfig` subclass (`url_schemes = ("datafusion",)`, `from_url`, `pool_key`, `build_sync_driver`, `build_async_driver`) resolved polymorphically via [`DatabaseRegistry`](../../../../../src/py/vertebra/db/registry.py)[^src-db-registry-py] and `provide_driver`. Validates `scheme == "datafusion"` (when present), `path in {"", "/"}`, `parse_qsl(..., keep_blank_values=True)`, `mode="in_process"`, `target_partitions >= 1`, and `memory_limit_mb >= 1`. |
| [`DataFusionCore`](../../../../../src/py/vertebra/db/adapters/datafusion/core.py)[^src-db-datafusion-core-py] | Builds the `SessionContext` from `target_partitions` (`SessionConfig.with_target_partitions`) and `memory_limit_mb` (`RuntimeEnvBuilder.with_fair_spill_pool`), normalizes `ArrowCursor` / PyArrow inputs (`coerce_arrow_table`), resolves `file://` URIs (`resolve_local_path`), registers Parquet, CSV, and NDJSON sources (`register_file_source`) plus `SyncObjectStore` Parquet objects (`register_store_parquet`), and formats planner-backed explain output (`format_explain_plan`). |
| [`DataFusionEngine`](../../../../../src/py/vertebra/db/adapters/datafusion/driver.py)[^src-db-datafusion-driver-py] | Accepts `(config: DataFusionConfig \| None = None, *, ctx: Any = None)`, wires `DataFusionCore` across `register_table`, `register_arrow_table`, `register_parquet_from_store`, `explain_plan`, and `execute_arrow`, resolves `SessionContext` via `ensure_module("datafusion", package="datafusion")`, and propagates `MissingDependencyError` and `SessionContext.sql(...)` errors directly. |

[^src-extensions-sqlspec-adapters-py]: `src/py/vertebra/extensions/sqlspec/_driver.py` — SQLSpec native pipeline dispatch and sequential stack execution.
[^src-db-duckdb-pipeline-py]: `src/py/vertebra/db/adapters/duckdb/pipeline.py` — Delayed-commit SyncDuckDBPipeline and AsyncDuckDBPipeline with savepoint nesting.
[^src-db-arrow-py]: `src/py/vertebra/db/arrow.py` — Fail-loud ArrowCursor, write_arrow_shm, read_arrow_shm, and Arrow C Stream export.
[^src-vertebra-py-db-rs]: `src/rs/vertebra-py/src/db.rs` — Native ArrowCursor.__arrow_c_stream__ PyCapsule export.
[^src-db-cdc-py]: `src/py/vertebra/db/cdc.py` — Engine-neutral ChangeFeed with locked _next_id increments and spill hydration.
[^src-db-rls-py]: `src/py/vertebra/db/rls.py` — TenantScopeConfig, TenantPredicate, and reset-or-evict tenant scopes.
[^src-db-datafusion-config-py]: `src/py/vertebra/db/adapters/datafusion/config.py` — DataFusionConfig and TableRegistration.
[^src-db-datafusion-core-py]: `src/py/vertebra/db/adapters/datafusion/core.py` — DataFusionCore session builder, source registration, and explain helpers.
[^src-db-datafusion-driver-py]: `src/py/vertebra/db/adapters/datafusion/driver.py` — DataFusionEngine federated Arrow SessionContext.
[^src-db-registry-py]: `src/py/vertebra/db/registry.py` — Polymorphic DatabaseRegistry URL and config resolution.
[^src-db-driver-capabilities-py]: `src/py/vertebra/db/driver/capabilities.py` — Sync/Async pipeline and change-source capability protocols.
[^src-db-postgres-notify-py]: `src/py/vertebra/db/adapters/postgres/notify.py` — PostgreSQL notification transport and logical message builders.
[^src-extensions-sqlspec-events-py]: `src/py/vertebra/extensions/sqlspec/events.py` — Engine-neutral SQLSpec change-feed event backends.
[^src-db-spanner-change-stream-py]: `src/py/vertebra/db/adapters/spanner/change_stream.py` — Spanner readers, constructor-bound stream defaults, record parsing, and child-partition scheduling.
[^src-extensions-sqlspec-bigquery-py]: `src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py` — BigQuery SQLSpec native statement defaults and session execution.
[^src-extensions-sqlspec-spanner-py]: `src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py` — Spanner SQLSpec parser and native parameter defaults.
[^src-postgres-tenant-core]: `src/py/vertebra/db/adapters/postgres/core.py` — PostgreSQL tenant and session SQL builders.
[^src-postgres-pipeline-py]: `src/py/vertebra/db/adapters/postgres/pipeline.py` — SyncPostgresPipeline and AsyncPostgresPipeline unified exit contract.
[^src-sqlite-pipeline-py]: `src/py/vertebra/db/adapters/sqlite/pipeline.py` — SyncSQLitePipeline and AsyncSQLitePipeline unified exit contract.
[^src-mysql-pipeline-py]: `src/py/vertebra/db/adapters/mysql/pipeline.py` — SyncMySQLPipeline and AsyncMySQLPipeline unified exit contract.
[^src-mssql-pipeline-py]: `src/py/vertebra/db/adapters/mssql/pipeline.py` — SyncMSSQLPipeline and AsyncMSSQLPipeline unified exit contract.
[^src-oracle-tenant-core]: `src/py/vertebra/db/adapters/oracle/core.py` — Oracle VPD trusted-package and SET_IDENTIFIER context SQL builders.
[^src-oracle-pipeline-py]: `src/py/vertebra/db/adapters/oracle/pipeline.py` — SyncOraclePipeline and AsyncOraclePipeline delayed-commit batching.
[^src-oracle-driver-py]: `src/py/vertebra/db/adapters/oracle/driver.py` — SyncOracleDriver and AsyncOracleDriver VPD reset-or-evict pool hooks.
[^src-db-oracle-notify-py]: `src/py/vertebra/db/adapters/oracle/notify.py` — OracleNotifyConfig and Sync/AsyncOracleNotificationSource TxEventQ / DBMS_AQ streaming.
[^src-vertebra-oracle-notify-rs]: `src/rs/vertebra-oracle/src/notify.rs` — Dedicated AsyncOracleListener DBMS_AQ.DEQUEUE worker pump.
[^src-extensions-apex-session-py]: `src/py/vertebra/extensions/apex/session.py` — OracleSessionManager APEX VPD propagation and ECID piggyback.
[^src-postgres-pgoutput-rs]: `src/rs/vertebra-postgres/src/replication/pgoutput.rs::PgOutputDecoder` — Incremental pgoutput v1 transaction records and bounded queue.
[^src-postgres-stream-rs]: `src/rs/vertebra-postgres/src/replication/stream.rs::ReplicationSession` — Bounded replication session, publication leases and shutdown.
[^src-postgres-supervisor-rs]: `src/rs/vertebra-postgres/src/replication/supervisor.rs::ReplicationSupervisor` — Generation-fenced reconnect supervision and identity verification.
[^src-db-postgres-change-stream-py]: `src/py/vertebra/db/adapters/postgres/change_stream.py::PostgresChangeSource` — Durable PostgreSQL CDC source with MemoryPostgresCheckpointStore and SqlPostgresCheckpointStore.
