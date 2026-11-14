---
type: Guide
title: "Native Pipelines, Change Feeds, Tenant Predicates & DataFusion"
description: "Native and delayed-commit pipeline capabilities, engine-neutral change feeds, PostgreSQL/Oracle notifications and Spanner change streams, VPD/RLS reset-or-evict tenant scopes, and DataFusion."
status: stable
tags:
  - domains
  - database
  - postgres
  - oracle
  - cdc
  - tenancy
generated:
  by: flow/0.30.1
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-14T20:49:30Z"
    commit: 838c54d1
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
    title: "OracleNotifyConfig and OracleNotificationSource TxEventQ / DBMS_AQ streaming"
  - id: src-vertebra-oracle-notify-rs
    resource: src/rs/vertebra-oracle/src/notify.rs
    title: "Dedicated AsyncOracleListener DBMS_AQ.DEQUEUE worker pump"
  - id: src-extensions-apex-session-py
    resource: src/py/vertebra/extensions/apex/session.py
    title: "OracleSessionManager APEX VPD propagation and ECID piggyback"
  - id: src-extensions-sqlspec-bigquery-py
    resource: src/py/vertebra/extensions/sqlspec/bigquery.py
    title: "BigQuery SQLSpec native statement defaults and session execution"
  - id: src-extensions-sqlspec-spanner-py
    resource: src/py/vertebra/extensions/sqlspec/spanner.py
    title: "Spanner SQLSpec parser and native parameter defaults"
  - id: src-extensions-sqlspec-adapters-py
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: "SQLSpec native pipeline dispatch and sequential stack execution"
  - id: src-db-duckdb-pipeline-py
    resource: src/py/vertebra/db/adapters/duckdb/pipeline.py
    title: "Delayed-commit SyncDuckDBPipeline and AsyncDuckDBPipeline with savepoint nesting"
  - id: src-db-cdc-py
    resource: src/py/vertebra/db/cdc.py
    title: "Engine-neutral ChangeFeed and spill hydration"
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
    title: "Durable PostgreSQL CDC source with shielded CAS checkpoints"
  - id: src-db-rls-py
    resource: src/py/vertebra/db/rls.py
    title: "TenantScopeConfig, TenantPredicate, and reset-or-evict tenant scopes"
  - id: src-db-datafusion-config-py
    resource: src/py/vertebra/db/adapters/datafusion/config.py
    title: "DataFusionConfig and TableRegistration"
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

# Native Pipelines, Change Feeds, Tenant Predicates & DataFusion

<!-- truth: start -->
- **Native PostgreSQL Replication**: `PgOutputDecoder::next_record` emits ordered v1 transaction envelopes without buffering entire transactions, `ReplicationSession` runs concurrent reader/writer loops (`1–1024` records, `8 MiB` / `16 MiB` bounds, cancel-safe delivery leases), `ReplicationSupervisor` provides generation-fenced identity-verified reconnects (`100ms..=5s`, `<= 8` attempts) into Python `AsyncPostgresReplication`, and [`PostgresChangeSource`](../../../../../src/py/vertebra/db/adapters/postgres/change_stream.py)[^src-db-postgres-change-stream-py] commits CAS checkpoints strictly before `acknowledge(lsn)` [^src-postgres-pgoutput-rs] [^src-postgres-stream-rs] [^src-postgres-supervisor-rs].
- **Delayed-Commit & Native Pipelines**: `SyncPipelineCapable` / `AsyncPipelineCapable` batch ordered statements via `driver.pipeline()`; `SyncOraclePipeline` and `AsyncOraclePipeline` (`ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED = False`) hold an open transaction or `SAVEPOINT _vtb_pipe_<n>` across `results()` and commit only on clean `__exit__`/`__aexit__`, while SQLSpec `OracleSyncDriver` / `OracleAsyncDriver` set `_execution_parameter_style = ParameterStyle.NAMED_COLON`.
- **Reset-or-Evict Tenant Scoping**: `TenantPredicate` is engine-neutral while `TenantConnectionScope` / `AsyncTenantConnectionScope` (`vertebra.db.rls`), `OracleCore` (`context_package_proc` / `context_clear_proc` + `DBMS_SESSION.SET_IDENTIFIER` / `CLEAR_IDENTIFIER`), and `OracleSessionManager` always reset session context on lease exit and permanently evict pooled sessions when reset fails.
- **Notifications & Change Streams**: `ChangeFeed` manages event envelopes and bounded spill hydration; `PostgresNotificationSource`, `OracleNotificationSource` (`AsyncOracleListener` `DBMS_AQ.DEQUEUE` / TxEventQ worker pump with 1s-sliced server waits and exponential reconnect backoff), and `SyncSpannerChangeStreamSource` / `AsyncSpannerChangeStreamSource` implement adapter streaming.
- **Federated DataFusion**: `DataFusionEngine` (`vertebra.db.adapters.datafusion`, `mode="in_process"` only) registers normalized `ArrowCursor` / PyArrow tables via `register_arrow_table` (`driver.select_to_arrow(...)`) on a fail-closed `SessionContext` (`ensure_module("datafusion", package="datafusion")`), and all SQL builders validate identifiers via `validate_identifier`.
<!-- truth: end -->

The native pipeline capability protocols and tenant scopes in [`vertebra.db`](../../../../../src/py/vertebra/db/__init__.py) define engine batching and session isolation contracts. [`cdc.py`](../../../../../src/py/vertebra/db/cdc.py)[^src-db-cdc-py] manages engine-neutral change-feed envelopes, [`postgres.notify`](../../../../../src/py/vertebra/db/adapters/postgres/notify.py)[^src-db-postgres-notify-py] owns PostgreSQL notification SQL, [`oracle.notify`](../../../../../src/py/vertebra/db/adapters/oracle/notify.py)[^src-db-oracle-notify-py] and [`vertebra-oracle/src/notify.rs`](../../../../../src/rs/vertebra-oracle/src/notify.rs)[^src-vertebra-oracle-notify-rs] own Oracle 26ai TxEventQ / `DBMS_AQ` streaming, [`spanner.change_stream`](../../../../../src/py/vertebra/db/adapters/spanner/change_stream.py)[^src-db-spanner-change-stream-py] owns Spanner change streams, and [`vertebra.db.adapters.datafusion`](../../../../../src/py/vertebra/db/adapters/datafusion/__init__.py) maintains federated analytical sessions. All SQL builders validate identifiers against `^[A-Za-z_][A-Za-z0-9_]*$` (`validate_identifier`, or AQ's `^[A-Za-z_][A-Za-z0-9_$#]{0,127}$`). Drivers and dialers live in [database-drivers-and-dialers](database-drivers-and-dialers.md).

## 1. Native Pipeline Capabilities

[`SyncPipelineCapable` / `AsyncPipelineCapable`](../../../../../src/py/vertebra/db/driver/capabilities.py)[^src-db-driver-capabilities-py] expose `driver.pipeline()` as a sync or async context manager (`pipe.select(sql, params)`, `pipe.execute(sql, params)`, `pipe.results()`). On Oracle (`ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED is False`), [`SyncOraclePipeline` and `AsyncOraclePipeline`](../../../../../src/py/vertebra/db/adapters/oracle/pipeline.py)[^src-oracle-pipeline-py] flush queued statements sequentially via `execute_pipeline_batch` (`OraclePipelineOutcome`) while holding an ambient transaction (or `SAVEPOINT _vtb_pipe_<n>` inside an outer transaction, using `ROLLBACK TO SAVEPOINT` with no `RELEASE SAVEPOINT`) open across `results()` and committing only on clean `__exit__`/`__aexit__` (`exc_type is None`); post-`results()` validation failures or unexecuted queued operations roll back automatically.

[`BaseSyncDriver.execute_stack` / `BaseAsyncDriver.execute_stack`](../../../../../src/py/vertebra/extensions/sqlspec/adapters.py)[^src-extensions-sqlspec-adapters-py] dispatch multi-statement `execute` stacks with `continue_on_error=False` through `driver.pipeline()`, preserving prepared operation types (including `INSERT ... RETURNING`) in ordered `StackResult` entries. Driver-owned statement defaults preserve native bind styles without importing optional upstream SDKs (shared by direct driver construction and SQLSpec adapter configs while preserving explicit caller configurations): PostgreSQL sync and async both compile to `$1..$N` numeric binds (`ParameterStyle.NUMERIC`) over `vertebra._postgres`, MySQL uses positional pyformat, `OracleSyncDriver` and `OracleAsyncDriver` set `_execution_parameter_style = ParameterStyle.NAMED_COLON`, BigQuery owns `bigquery` named-`@` parameters, and Spanner selects `spanner` named-`@` (GoogleSQL) or `spangres` numeric parameters.[^src-extensions-sqlspec-bigquery-py][^src-extensions-sqlspec-spanner-py]

Batch stack operations compile batched parameters through SQLSpec's `is_many` processing before native execution, applying statement-config overrides and filters. Arrow stack operations prepare keyword or prebound SQL parameters through the same statement pipeline, separate class-owned execution options (`selected_fields`, `row_restriction`, `max_streams` on BigQuery; `data_boost`, `max_partitions`, `staleness` on Spanner) from bind values without engine-name dispatch, and materialize native `ArrowCursor` results through `to_arrow_table()` into SQLSpec `ArrowResult` at the stack boundary.

Single-operation stacks, non-pipeline drivers, stacks containing `execute_many`, `execute_script`, or `execute_arrow`, and `continue_on_error=True` use adapter primitives sequentially. Fail-fast execution uses one transaction and rolls back on failure. Continue-on-error execution commits or rolls back each operation, records an error result at the failed operation's position, and continues. Raw Spanner all-DML stacks execute via `execute_batch_dml`, raw BigQuery stacks execute inside session transactions, and [`SyncDuckDBPipeline` / `AsyncDuckDBPipeline`](../../../../../src/py/vertebra/db/adapters/duckdb/pipeline.py)[^src-db-duckdb-pipeline-py] batch queued `execute`/`select` operations atomically with delayed commit on normal context exit or `_vtb_pipe_<n>` savepoint isolation when `driver.in_transaction` is already `True`.

## 2. Tenant Isolation (`rls.py`)

| Symbol | Responsibility |
|---|---|
| [`TenantScopeConfig` / `TenantPredicate`](../../../../../src/py/vertebra/db/rls.py)[^src-db-rls-py] | Frozen engine-neutral scope (`setting_name="app.tenant_id"`, `tenant_column="tenant_id"`, `parameter_cast=None`) and predicate (`table_alias=""`, `tenant_value`, `column_reference()`, ANSI `to_sql_fragment(placeholder="?")` applying optional `CAST(<placeholder> AS <type>)` to preserve column indexes). |
| [`TenantConnectionScope` / `AsyncTenantConnectionScope`](../../../../../src/py/vertebra/db/rls.py)[^src-db-rls-py] | Sync/async context managers calling `driver.enter_tenant_scope(tenant_value, config)` on entry and guaranteeing `driver.exit_tenant_scope(config)` on exit; if reset raises, invokes `driver.evict_connection()` (falling back to `close()`) before re-raising so tainted sessions never return to a pool. |
| [`PostgreSQLCore`](../../../../../src/py/vertebra/db/adapters/postgres/core.py)[^src-postgres-tenant-core] | `build_session_guc_sql` binds `(setting_name, tenant_value, local)` in `SELECT set_config($1, $2, $3)`; `build_tenant_predicate_sql` renders `$N` with optional `::<type>` cast. |
| [`OracleCore`](../../../../../src/py/vertebra/db/adapters/oracle/core.py)[^src-oracle-tenant-core] | `build_session_context_sql` and `build_clear_context_sql` route `OracleConfig.context_package_proc` / `context_clear_proc` (`validate_identifier(..., allow_dots=True)`) to trusted PL/SQL procedures (`BEGIN <proc>(:namespace, :attribute, :value); END;`) and default `namespace="USERENV"` to `DBMS_SESSION.SET_IDENTIFIER(:value)` / `DBMS_SESSION.CLEAR_IDENTIFIER` so Oracle 26ai never fails with unprivileged `ORA-01031`. |

`BaseSyncDriver.apply_tenant_predicate` and its async counterpart refuse unsupported engines with native `NotSupportedError`. PostgreSQL sync and async adapters both compile session GUC SQL (`SELECT set_config($1, $2, $3)`) through `ParameterStyle.NUMERIC` `$N` binds and unwrap typed SQLSpec parameters before native `vertebra._postgres` transport. Oracle adapters bind the context setter and return a named predicate, propagating original setter exceptions unchanged.

BigQuery and Spanner's eight SQLSpec config/driver constructors reject a non-None `tenant_predicate` as their first executable statement, before base initialization, core access, config resolution, broker allocation, or native side effects. Unsupported isolation sessions cannot be constructed; unconfigured sessions preserve query, script, Arrow, batch, and snapshot behavior.

Because Oracle `SYS_CONTEXT` and `USERENV.CLIENT_IDENTIFIER` survive `ROLLBACK`, [`SyncOracleDriver` / `AsyncOracleDriver`](../../../../../src/py/vertebra/db/adapters/oracle/driver.py)[^src-oracle-driver-py] (`enter_tenant_scope`, `exit_tenant_scope`, `evict_connection`) and [`OracleSessionManager`](../../../../../src/py/vertebra/extensions/apex/session.py)[^src-extensions-apex-session-py] (`build_attach_plsql`, `build_detach_plsql`, `acquire_scoped`, plus TTC `ECID` `set_session_attrs` piggyback) execute explicit clear procedures before pool release and permanently evict the physical session if detach or reset raises. Live Oracle `DBMS_SESSION.SET_CONTEXT` execution requires a trusted package associated with `CREATE CONTEXT` in the call stack, and no helper provisions RLS or VPD policies ([Oracle DBMS_SESSION reference](https://docs.oracle.com/en/database/oracle/oracle-database/26/arpls/DBMS_SESSION.html)).

### Generic Database Engine Purity

The AST gate scans `db/*.py` and `db/driver/*.py` for adapter imports, executable adapter-class use, literal adapter loading, and engine-specific SQL construction. It excludes docstrings, annotations, and engine metadata. The registry permits only module-level adapter config imports directly listed in `REGISTERED_CONFIG_TYPES`, their declarative tuple and annotation/type-alias uses, and guarded `TYPE_CHECKING` driver imports used in annotations/type aliases. `SCHEME_REGISTRY` derives URL schemes from that tuple. Runtime driver imports, engine-class branches, identity dispatch, and direct construction are forbidden.

Cloud SQL and AlloyDB transport dialers belong in `db.dialer` with dedicated registry branches. Polymorphic `config_cls.from_url`, `config.build_sync_driver`, `config.build_async_driver`, and `ensure_module(self.driver_module)` provide configuration-owned driver resolution across stable broker dialect IDs. [^src-db-registry-py]

## 3. Change Feed (`cdc.py`)

| Symbol | Responsibility |
|---|---|
| [`ChangeEvent`](../../../../../src/py/vertebra/db/cdc.py)[^src-db-cdc-py] | `FrozenBaseStruct(frozen=True, gc=False, kw_only=True)`: `event_id`, `channel`, `operation`, `payload`, `spilled`. |
| `CDCConfig` | Engine-neutral frozen configuration: `channel="vertebra_events"`, `spill_threshold_bytes=7500`, `spill_prefix="cdc/spill"`, `max_history_size=1024`, `max_memory_spills=1024`. `max_memory_spills` must be at least `1`. |
| `ChangeFeed.publish_event(operation, payload)` | Assigns a monotonically increasing `event_id` and serializes with `encode_json_bytes`. Payloads strictly larger than the byte threshold spill to an attached `SyncObjectStore` or bounded in-memory `OrderedDict`; the wire envelope carries `spilled: true` and `spill_key` without the payload. Returns `(wire_envelope, ChangeEvent)` and appends to bounded replay history. |
| `ChangeFeed.hydrate_notification(envelope)` | Decodes with `decode_json`, restores inline payloads, or hydrates spills from `SyncObjectStore` or the memory cache. Successful memory hydration refreshes LRU recency. Store errors propagate directly; an evicted or unknown memory spill raises `KeyError("Evicted or unknown in-memory CDC spill id: <id>")`. |
| `ChangeFeed.replay_since(last_event_id)` | Returns buffered events newer than the id. |
| [`PostgresNotifyConfig`](../../../../../src/py/vertebra/db/adapters/postgres/notify.py)[^src-db-postgres-notify-py] | Frozen PostgreSQL notification configuration: `channel="vertebra_events"`, `spill_threshold_bytes=7500`, `spill_table="vertebra_notify_spill"`, `spill_prefix="cdc/spill"`, `replication_slot_name="vertebra_cdc_slot"`, `publication_name="vertebra_pub"`, `max_slot_wal_keep_size_mb=4096`. Validates channel, table, slot, and publication as single SQL identifiers. |
| `PostgresNotificationSource` | Implements `NotificationSource` with async `publish`, `subscribe`, `unsubscribe`, and `listen`. Publishes through an async driver directly or a sync driver via `asyncio.to_thread`. With a `ChangeFeed`, publishes canonical `PUBLISH` envelopes containing `channel` and base64 `data_b64`; otherwise strings pass through and bytes use a `b64:` prefix. Subscription identifiers are validated. Sync and async drivers expose `listen_notifications(config=None, *, change_feed=None, queue_capacity=1024, reconnect=False) -> PostgresNotificationSource`, and `PostgresNotificationSource` delegates iteration to `driver._open_notification_listener(tuple(subscribed_channels), queue_capacity=..., reconnect=...)` or raises `NotSupportedError` when unavailable. |
| `PostgresNotificationSource.build_emit_message_sql` (static) | Returns `("SELECT pg_logical_emit_message($1, $2, $3)", (transactional, prefix, payload_bytes))` for logical WAL messages. `ChangeFeed` and the SQLSpec event backends expose no logical message SQL builder. |
| `PostgresNotificationSource.build_notify_sql` (static) | Validates the channel and returns `("SELECT pg_notify($1, $2)", (channel, payload))` with bound values. |
| [`PostgresChangeSource`](../../../../../src/py/vertebra/db/adapters/postgres/change_stream.py)[^src-db-postgres-change-stream-py] | Durable `AsyncChangeSource` over `AsyncPostgresReplication`: runs `PostgresCheckpointStore` (`MemoryPostgresCheckpointStore`) single-flight shielded CAS checkpoint commits strictly before `acknowledge(lsn)`, raising `PostgresReplicationGapError` or `PostgresCheckpointConflictError` fail-closed. |
| [`OracleNotifyConfig` / `OracleNotificationSource`](../../../../../src/py/vertebra/db/adapters/oracle/notify.py)[^src-db-oracle-notify-py] & [`AsyncOracleListener`](../../../../../src/rs/vertebra-oracle/src/notify.rs)[^src-vertebra-oracle-notify-rs] | Implements `@runtime_checkable NotificationSource` (`publish`, `subscribe`, `unsubscribe`, `listen() -> AsyncIterator[NotificationEvent]`) plus `subscribe_aq`/`publish_aq`/`listen_aq`/`close`. Validates AQ queue identifiers (`^[A-Za-z_][A-Za-z0-9_$#]{0,127}$`, `ProgrammingError` `42602`), runs 1s-sliced server-blocking `DBMS_AQ.DEQUEUE` / TxEventQ iterations on a dedicated non-pooled Rust worker thread (`ORA-25228` idle slice loop, `1024`-frame oldest-drop buffer, immediate enqueue wakeup and `<=1s` clean exit on `unsubscribe`/`close` without issuing `ALTER SYSTEM CANCEL SQL` against `oracledb` `26.0.0-beta.5`'s unimplemented AQ `ErrorInfo::deserialize` path), retries transient disconnects with exponential backoff (`reconnect_initial_delay=0.25` -> `reconnect_max_delay=15.0`, `max_reconnect_attempts=5`), and hydrates optional `ChangeFeed` envelopes. |
| [`ChangeFeedSyncEventBackend` / `ChangeFeedAsyncEventBackend`](../../../../../src/py/vertebra/extensions/sqlspec/events.py)[^src-extensions-sqlspec-events-py] | Engine-neutral SQLSpec event backends support `publish`, `publish_many`, spill-hydrating `dequeue`, `ack`, `nack` redelivery, and `shutdown`. |
| [`SyncSpannerChangeStreamSource` / `AsyncSpannerChangeStreamSource`](../../../../../src/py/vertebra/db/adapters/spanner/change_stream.py)[^src-db-spanner-change-stream-py] / [`SpannerChildPartition` / `SpannerChangeStreamRecord`](../../../../../src/py/vertebra/db/adapters/spanner/change_stream.py) | Implements the [`SyncChangeSource` / `AsyncChangeSource`](../../../../../src/py/vertebra/db/driver/capabilities.py)[^src-db-driver-capabilities-py] capability protocols with zero-argument `stream()` calls. Constructors bind `start_timestamp`, `end_timestamp`, `partition_token`, and `heartbeat_milliseconds`; the async constructor also binds `poll_interval_seconds`. Calls may override stream defaults; absent start timestamps resolve to the current UTC time. Sync readers select rows directly and async readers await selection directly without awaitable inspection, querying `READ_<stream_name>` in GoogleSQL (`spanner.read_json_<stream_name>` is Spangres-only), parsing `DataChangeRecord`, `HeartbeatRecord`, and `ChildPartitionsRecord` payloads into `SpannerChangeStreamRecord` and `SpannerChildPartition` structs, fanning out child partition readers respecting parent watermark progression, and forwarding data changes into `ChangeFeed.publish_event()`. Spangres polls bind `[start_timestamp, end_timestamp, partition_token, heartbeat_milliseconds]` positionally (`$1..$4` → `p1..p4`); GoogleSQL polls bind `@start_ts`, `@end_ts`, `@token`, and `@heartbeat_ms`. |

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
| `DataFusionConfig` | `BaseDatabaseConfig` subclass (`url_schemes = ("datafusion",)`, `from_url`, `pool_key`, `build_sync_driver`, `build_async_driver`) resolved polymorphically via [`DatabaseRegistry`](../../../../../src/py/vertebra/db/registry.py)[^src-db-registry-py] and `provide_driver`. Configures `mode="in_process"`, `target_partitions >= 1`, and `memory_limit_mb >= 1`. |
| [`DataFusionEngine`](../../../../../src/py/vertebra/db/adapters/datafusion/driver.py)[^src-db-datafusion-driver-py] | Accepts `(config: DataFusionConfig \| None = None, *, ctx: Any = None)`, registers relational and cloud `ArrowCursor` / PyArrow tables via `register_arrow_table(name, driver.select_to_arrow(...))` (zero vendor-specific registration wrappers), resolves `SessionContext` via `ensure_module("datafusion", package="datafusion")`, and propagates `MissingDependencyError` and `SessionContext.sql(...)` errors directly. |

[^src-extensions-sqlspec-adapters-py]: `src/py/vertebra/extensions/sqlspec/adapters.py` — SQLSpec native pipeline dispatch and sequential stack execution.
[^src-db-duckdb-pipeline-py]: `src/py/vertebra/db/adapters/duckdb/pipeline.py` — Delayed-commit SyncDuckDBPipeline and AsyncDuckDBPipeline with savepoint nesting.
[^src-db-cdc-py]: `src/py/vertebra/db/cdc.py` — Engine-neutral ChangeFeed and spill hydration.
[^src-db-rls-py]: `src/py/vertebra/db/rls.py` — TenantScopeConfig, TenantPredicate, and reset-or-evict tenant scopes.
[^src-db-datafusion-config-py]: `src/py/vertebra/db/adapters/datafusion/config.py` — DataFusionConfig and TableRegistration.
[^src-db-datafusion-driver-py]: `src/py/vertebra/db/adapters/datafusion/driver.py` — DataFusionEngine federated Arrow SessionContext.
[^src-db-registry-py]: `src/py/vertebra/db/registry.py` — Polymorphic DatabaseRegistry URL and config resolution.
[^src-db-driver-capabilities-py]: `src/py/vertebra/db/driver/capabilities.py` — Sync/Async pipeline and change-source capability protocols.
[^src-db-postgres-notify-py]: `src/py/vertebra/db/adapters/postgres/notify.py` — PostgreSQL notification transport and logical message builders.
[^src-extensions-sqlspec-events-py]: `src/py/vertebra/extensions/sqlspec/events.py` — Engine-neutral SQLSpec change-feed event backends.
[^src-db-spanner-change-stream-py]: `src/py/vertebra/db/adapters/spanner/change_stream.py` — Spanner readers, constructor-bound stream defaults, record parsing, and child-partition scheduling.
[^src-extensions-sqlspec-bigquery-py]: `src/py/vertebra/extensions/sqlspec/bigquery.py` — BigQuery SQLSpec native statement defaults and session execution.
[^src-extensions-sqlspec-spanner-py]: `src/py/vertebra/extensions/sqlspec/spanner.py` — Spanner SQLSpec parser and native parameter defaults.
[^src-postgres-tenant-core]: `src/py/vertebra/db/adapters/postgres/core.py` — PostgreSQL tenant and session SQL builders.
[^src-oracle-tenant-core]: `src/py/vertebra/db/adapters/oracle/core.py` — Oracle VPD trusted-package and SET_IDENTIFIER context SQL builders.
[^src-oracle-pipeline-py]: `src/py/vertebra/db/adapters/oracle/pipeline.py` — SyncOraclePipeline and AsyncOraclePipeline delayed-commit batching.
[^src-oracle-driver-py]: `src/py/vertebra/db/adapters/oracle/driver.py` — SyncOracleDriver and AsyncOracleDriver VPD reset-or-evict pool hooks.
[^src-db-oracle-notify-py]: `src/py/vertebra/db/adapters/oracle/notify.py` — OracleNotifyConfig and OracleNotificationSource TxEventQ / DBMS_AQ streaming.
[^src-vertebra-oracle-notify-rs]: `src/rs/vertebra-oracle/src/notify.rs` — Dedicated AsyncOracleListener DBMS_AQ.DEQUEUE worker pump.
[^src-extensions-apex-session-py]: `src/py/vertebra/extensions/apex/session.py` — OracleSessionManager APEX VPD propagation and ECID piggyback.
[^src-postgres-pgoutput-rs]: `src/rs/vertebra-postgres/src/replication/pgoutput.rs::PgOutputDecoder` — Incremental pgoutput v1 transaction records and bounded queue.
[^src-postgres-stream-rs]: `src/rs/vertebra-postgres/src/replication/stream.rs::ReplicationSession` — Bounded replication session, publication leases and shutdown.
[^src-postgres-supervisor-rs]: `src/rs/vertebra-postgres/src/replication/supervisor.rs::ReplicationSupervisor` — Generation-fenced reconnect supervision and identity verification.
[^src-db-postgres-change-stream-py]: `src/py/vertebra/db/adapters/postgres/change_stream.py::PostgresChangeSource` — Durable PostgreSQL CDC source with shielded CAS checkpoints.
