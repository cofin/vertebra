---
type: Guide
title: "Native Pipelines, Change Feeds, Tenant Predicates & DataFusion"
description: "Native pipeline capabilities, engine-neutral change feeds, adapter-owned PostgreSQL notifications and Spanner change streams, tenant predicates, and the DataFusionEngine table catalog."
status: stable
tags: [domains, database, postgres, cdc, tenancy]
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-extensions-sqlspec-bigquery-py
    resource: src/py/vertebra/extensions/sqlspec/bigquery.py
    title: "BigQuery SQLSpec native statement defaults and session execution"
  - id: src-extensions-sqlspec-spanner-py
    resource: src/py/vertebra/extensions/sqlspec/spanner.py
    title: "Spanner SQLSpec parser and native parameter defaults"
  - id: src-extensions-sqlspec-adapters-py
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: "SQLSpec native pipeline dispatch and sequential stack execution"
  - id: src-db-cdc-py
    resource: src/py/vertebra/db/cdc.py
    title: "Python implementation module (src/py/vertebra/db/cdc.py)"
  - id: src-db-spanner-change-stream-py
    resource: src/py/vertebra/db/adapters/spanner/change_stream.py
    title: "Spanner change-stream readers and partition record types"
  - id: src-db-postgres-notify-py
    resource: src/py/vertebra/db/adapters/postgres/notify.py
    title: "PostgreSQL notification transport and logical message builders"
  - id: src-extensions-sqlspec-events-py
    resource: src/py/vertebra/extensions/sqlspec/events.py
    title: "Engine-neutral SQLSpec change-feed event backends"
  - id: src-tests-unit-db-test-pg-gateway-py
    resource: src/py/tests/unit/db/test_pg_gateway.py
    title: "CDC spill bounds, key normalization, and storage error coverage"
  - id: src-tests-unit-extensions-test-data-storage-integrations-py
    resource: src/py/tests/unit/extensions/test_data_storage_integrations.py
    title: "SQLSpec event backend lifecycle and spill hydration coverage"
  - id: src-db-rls-py
    resource: src/py/vertebra/db/rls.py
    title: "Python implementation module (src/py/vertebra/db/rls.py)"
  - id: src-db-datafusion-py
    resource: src/py/vertebra/db/adapters/datafusion/__init__.py
    title: "Python implementation module (src/py/vertebra/db/adapters/datafusion/__init__.py)"
  - id: src-db-datafusion-config-py
    resource: src/py/vertebra/db/adapters/datafusion/config.py
    title: "Python implementation module (src/py/vertebra/db/adapters/datafusion/config.py)"
  - id: src-db-datafusion-driver-py
    resource: src/py/vertebra/db/adapters/datafusion/driver.py
    title: "Python implementation module (src/py/vertebra/db/adapters/datafusion/driver.py)"
  - id: src-db-init-py
    resource: src/py/vertebra/db/__init__.py
    title: "Python implementation module (src/py/vertebra/db/__init__.py)"
  - id: src-db-registry-py
    resource: src/py/vertebra/db/registry.py
    title: "Python implementation module (src/py/vertebra/db/registry.py)"
  - id: src-db-driver-capabilities-py
    resource: src/py/vertebra/db/driver/capabilities.py
    title: "Python implementation module (src/py/vertebra/db/driver/capabilities.py)"
  - id: src-tests-unit-db-test-pg-dataplane-and-datafusion-py
    resource: src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
    title: "Python implementation module (src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py)"
---

# Native Pipelines, Change Feeds, Tenant Predicates & DataFusion

<!-- truth: start -->
- `ChangeFeed` provides engine-neutral event envelopes, bounded spill hydration, replay, and Parquet archival; PostgreSQL notification SQL lives in `vertebra.db.adapters.postgres.notify`.
- `SyncSpannerChangeStreamSource` / `AsyncSpannerChangeStreamSource` live in `vertebra.db.adapters.spanner.change_stream` with constructor-bound defaults and zero-argument capability streams; `TenantPredicate` provides fail-loud row-level security session scoping.
- `DataFusionEngine` (`vertebra.db.adapters.datafusion`) registers normalized PyArrow, BigQuery, and Spanner tables on a fail-closed `SessionContext` (`ensure_module("datafusion", package="datafusion")`), and all SQL builders validate identifiers via `validate_identifier`.
<!-- truth: end -->

The native pipeline capability protocols and tenant predicates in [`vertebra.db`](../../../../../src/py/vertebra/db/__init__.py)[^src-db-init-py] provide engine batching contracts and PostgreSQL tenant SQL text with bind tuples. The `cdc` module manages engine-neutral change-feed envelopes, [`spanner.change_stream`](../../../../../src/py/vertebra/db/adapters/spanner/change_stream.py)[^src-db-spanner-change-stream-py] owns Spanner change streams, [`postgres.notify`](../../../../../src/py/vertebra/db/adapters/postgres/notify.py)[^src-db-postgres-notify-py] owns PostgreSQL notification SQL, and the [`vertebra.db.adapters.datafusion`](../../../../../src/py/vertebra/db/adapters/datafusion/__init__.py)[^src-db-datafusion-py] adapter maintains federated analytical session state. Identifiers interpolated into generated SQL are validated against `^[A-Za-z_][A-Za-z0-9_]*$` (`_validate_identifier`). Drivers and connection configs are covered in [database-drivers-and-dialers](database-drivers-and-dialers.md).

## 1. Native Pipeline Capabilities

[`SyncPipelineCapable` / `AsyncPipelineCapable`](../../../../../src/py/vertebra/db/driver/capabilities.py)[^src-db-driver-capabilities-py] expose `driver.pipeline()` as a sync or async context manager. Queue row-returning statements with `pipe.select(sql, params)` and count-returning statements with `pipe.execute(sql, params)`. `pipe.results()` (awaited for async) flushes queued work and returns results in queue order.

[`BaseSyncDriver.execute_stack` / `BaseAsyncDriver.execute_stack`](../../../../../src/py/vertebra/extensions/sqlspec/adapters.py)[^src-extensions-sqlspec-adapters-py] use the native capability for more than one `execute` operation when `continue_on_error=False`. SQLSpec prepares SQL and binds through upstream statement processing and uses processed row metadata for dispatch. Each operation retains its own rows or affected count in an ordered `StackResult`. Row results retain the prepared statement's operation type, including `INSERT` for `INSERT ... RETURNING`; driver and preparation errors propagate.

Driver-owned default statement configuration preserves native execution syntax: PostgreSQL sync uses positional pyformat and async uses numeric binds; MySQL sync and async use positional pyformat. The private default hook is shared by direct driver construction and SQLSpec adapter configs. Explicit caller configurations retain identity and control processing. BigQuery owns its `bigquery` parser and named-at parameters; Spanner owns its supported `spanner` parser and named-at parameters for GoogleSQL, with `spangres` parsing and numeric parameters for the resolved Spangres configuration. These defaults preserve SQLSpec row metadata and native client binds without importing optional upstream adapter SDKs. [^src-extensions-sqlspec-bigquery-py][^src-extensions-sqlspec-spanner-py]

Batch stack operations compile batched parameters through SQLSpec's `is_many` processing before native execution, retaining statement-config overrides and filters. Arrow stack operations prepare keyword or prebound SQL parameters through the same statement pipeline, keep standard Arrow options separate from bind values, and retain prepared statement metadata. Native BigQuery and Spanner `ArrowCursor` results are materialized through `to_arrow_table()` into SQLSpec `ArrowResult` at the stack boundary. Standalone Arrow option semantics remain owned by each adapter.

Arrow stack execution separates the owning driver's public execution options from bind values before SQLSpec preparation. BigQuery retains `selected_fields`, `row_restriction`, and `max_streams`; Spanner retains `data_boost`, `max_partitions`, and `staleness`, alongside the standard Arrow options. These private class-owned declarations preserve native calls without engine-name dispatch or per-operation reflection.

Single-operation stacks, non-pipeline drivers, stacks containing `execute_many`, `execute_script`, or `execute_arrow`, and `continue_on_error=True` use adapter primitives sequentially. Fail-fast execution uses one transaction and rolls back on failure. Continue-on-error execution commits or rolls back each operation, retains an error result at the failed operation's position, and continues. Raw Spanner all-DML stacks retain `execute_batch_dml`, and raw BigQuery stacks retain session transactions.

## 2. Tenant Isolation (`rls.py`)

| Symbol | Responsibility |
|---|---|
| [`TenantScopeConfig`](../../../../../src/py/vertebra/db/rls.py)[^src-db-rls-py] | `guc_name="app.tenant_id"`, `tenant_column="tenant_id"`, `parameter_cast="uuid"`. Validated in `__post_init__` via `SqlIdentifier.validate`. |
| `TenantPredicate` | `to_sql_fragment(param_index)` validates `table_alias`, `tenant_column`, and `parameter_cast` via `SqlIdentifier.validate` and renders `alias.tenant_id = $N::uuid`. The cast goes on the parameter, never the column, so the planner can still use an index on `tenant_id`. An empty `table_alias` drops the prefix. |
| `TenantPredicate.build_session_guc_sql` (static) | Returns `("SELECT set_config($1, $2, $3)", (guc_name, tenant_id, is_local))` for a transaction-local GUC that RLS policies can read. The sqlspec adapter drivers run it in `apply_tenant_predicate` and propagate any driver error. |

The intended layering is an explicit predicate for plan quality plus a session GUC for row-level-security policies. Both pieces are SQL fragments only. Nothing in the codebase installs RLS policies.

## 3. Change Feed (`cdc.py`)

| Symbol | Responsibility |
|---|---|
| [`ChangeEvent`](../../../../../src/py/vertebra/db/cdc.py)[^src-db-cdc-py] | `FrozenBaseStruct(frozen=True, gc=False, kw_only=True)`: `event_id`, `channel`, `operation`, `payload`, `spilled`. |
| `CDCConfig` | Engine-neutral frozen configuration: `channel="vertebra_events"`, `spill_threshold_bytes=7500`, `spill_prefix="cdc/spill"`, `max_history_size=1024`, `max_memory_spills=1024`. `max_memory_spills` must be at least `1`. |
| `ChangeFeed.publish_event(operation, payload)` | Assigns a monotonically increasing `event_id` and serializes with `encode_json_bytes`. Payloads strictly larger than the byte threshold spill to an attached `SyncObjectStore` or bounded in-memory `OrderedDict`; the wire envelope carries `spilled: true` and `spill_key` without the payload. Returns `(wire_envelope, ChangeEvent)` and appends to bounded replay history. |
| `ChangeFeed.hydrate_notification(envelope)` | Decodes with `decode_json`, restores inline payloads, or hydrates spills from `SyncObjectStore` or the memory cache. Successful memory hydration refreshes LRU recency. Store errors propagate directly; an evicted or unknown memory spill raises `KeyError("Evicted or unknown in-memory CDC spill id: <id>")`. |
| `ChangeFeed.replay_since(last_event_id)` | Returns buffered events newer than the id. |
| [`PostgresNotifyConfig`](../../../../../src/py/vertebra/db/adapters/postgres/notify.py)[^src-db-postgres-notify-py] | Frozen PostgreSQL notification configuration: `channel="vertebra_events"`, `spill_threshold_bytes=7500`, `spill_table="vertebra_notify_spill"`, `spill_prefix="cdc/spill"`, `replication_slot_name="vertebra_cdc_slot"`, `publication_name="vertebra_pub"`, `max_slot_wal_keep_size_mb=4096`. Validates channel, table, slot, and publication as single SQL identifiers. |
| `PostgresNotificationSource` | Implements `NotificationSource` with async `publish`, `subscribe`, `unsubscribe`, and `listen`. Publishes through an async driver directly or a sync driver via `asyncio.to_thread`. With a `ChangeFeed`, publishes canonical `PUBLISH` envelopes containing `channel` and base64 `data_b64`; otherwise strings pass through and bytes use a `b64:` prefix. Subscription identifiers are validated. Streaming delegates to `driver.listen_notifications(tuple(channels))` or raises `NotSupportedError` on iteration when unavailable. |
| `PostgresNotificationSource.build_emit_message_sql` (static) | Returns `("SELECT pg_logical_emit_message($1, $2, $3)", (transactional, prefix, payload_bytes))` for logical WAL messages. `ChangeFeed` and the SQLSpec event backends expose no logical message SQL builder. |
| `PostgresNotificationSource.build_notify_sql` (static) | Validates the channel and returns `("SELECT pg_notify($1, $2)", (channel, payload))` with bound values. |
| [`ChangeFeedSyncEventBackend` / `ChangeFeedAsyncEventBackend`](../../../../../src/py/vertebra/extensions/sqlspec/events.py)[^src-extensions-sqlspec-events-py] | Engine-neutral SQLSpec event backends support `publish`, `publish_many`, spill-hydrating `dequeue`, `ack`, `nack` redelivery, and `shutdown`. |
| [`SyncSpannerChangeStreamSource` / `AsyncSpannerChangeStreamSource`](../../../../../src/py/vertebra/db/adapters/spanner/change_stream.py)[^src-db-spanner-change-stream-py] / [`SpannerChildPartition` / `SpannerChangeStreamRecord`](../../../../../src/py/vertebra/db/adapters/spanner/change_stream.py) | Implements the [`SyncChangeSource` / `AsyncChangeSource`](../../../../../src/py/vertebra/db/driver/capabilities.py)[^src-db-driver-capabilities-py] capability protocols with zero-argument `stream()` calls. Constructors bind `start_timestamp`, `end_timestamp`, `partition_token`, and `heartbeat_milliseconds`; the async constructor also binds `poll_interval_seconds`. Calls may override stream defaults; absent start timestamps resolve to the current UTC time. Sync readers select rows directly and async readers await selection directly without awaitable inspection, querying `READ_<stream_name>` in GoogleSQL (`spanner.read_json_<stream_name>` is Spangres-only), parsing `DataChangeRecord`, `HeartbeatRecord`, and `ChildPartitionsRecord` payloads into `SpannerChangeStreamRecord` and `SpannerChildPartition` structs, fanning out child partition readers respecting parent watermark progression, and forwarding data changes into `ChangeFeed.publish_event()`. Spangres polls bind `[start_timestamp, end_timestamp, partition_token, heartbeat_milliseconds]` positionally (`$1..$4` → `p1..p4`); GoogleSQL polls bind `@start_ts`, `@end_ts`, `@token`, and `@heartbeat_ms`. |

Replay history retains at most `max_history_size` events. Without an object store, spill payloads use an LRU cache bounded by `max_memory_spills`; publication evicts the least recently used spill when the bound is exceeded. Hydration refreshes recency, and an evicted envelope fails explicitly instead of returning an empty payload.

Store keys use `spill_prefix.rstrip("/")`: a non-empty normalized prefix yields `<prefix>/<event_id>.json`, and an empty prefix yields `<event_id>.json`. Publication and hydration without an explicit `spill_key` apply the same normalization. `SyncObjectStore.get()` exceptions, including missing-object errors, reach the caller unchanged. PostgreSQL replication and notification settings belong to `PostgresNotifyConfig`, while `CDCConfig` retains only engine-neutral fields.

## 4. DataFusion Federation Adapter (`adapters/datafusion/`)

| Symbol | Responsibility |
|---|---|
| `ARROW_STREAM_MEDIA_TYPE` | `"application/vnd.apache.arrow.stream"`, also exposed as `DataFusionEngine.ARROW_STREAM_MEDIA_TYPE`. |
| `DataFusionMode` | `Literal["in_process", "pgwire_fdw"]`. |
| [`TableRegistration`](../../../../../src/py/vertebra/db/adapters/datafusion/config.py)[^src-db-datafusion-config-py] | `name`, `uri`, `format` (`parquet`, `vortex`, `arrow`, `csv`, `ndjson`, `duckdb`, `postgres`, `oracle`, `bigquery`, `spanner`). |
| `DataFusionConfig` | `BaseDatabaseConfig` subclass (`url_schemes = ("datafusion",)`, `from_url`, `pool_key`, `build_sync_driver`, `build_async_driver`) resolved polymorphically via [`DatabaseRegistry`](../../../../../src/py/vertebra/db/registry.py)[^src-db-registry-py] and `provide_driver`. Configures `mode`, `target_partitions`, `memory_limit_mb`, `pgwire_host`, `pgwire_port=5434`, and `require_mtls_for_pgwire`. |
| [`DataFusionEngine`](../../../../../src/py/vertebra/db/adapters/datafusion/driver.py)[^src-db-datafusion-driver-py] | Accepts `(config: DataFusionConfig \| None = None, *, ctx: Any = None)`, fetches BigQuery and Cloud Spanner datasets via `driver.select_to_arrow(...)`, normalizes `ArrowCursor` / PyArrow inputs into `pyarrow.Table`, resolves `SessionContext` via `ensure_module("datafusion", package="datafusion")` without any SQLite fallback, and propagates `MissingDependencyError` and `SessionContext.sql(...)` errors directly. |

Provenance sources: [^src-tests-unit-db-test-pg-dataplane-and-datafusion-py], [^src-tests-unit-db-test-pg-gateway-py], [^src-tests-unit-extensions-test-data-storage-integrations-py].

[^src-extensions-sqlspec-adapters-py]: `src/py/vertebra/extensions/sqlspec/adapters.py` — SQLSpec native pipeline dispatch and sequential stack execution.
[^src-db-cdc-py]: `src/py/vertebra/db/cdc.py` — Python implementation module (src/py/vertebra/db/cdc.py).
[^src-db-rls-py]: `src/py/vertebra/db/rls.py` — Python implementation module (src/py/vertebra/db/rls.py).
[^src-db-datafusion-py]: `src/py/vertebra/db/adapters/datafusion/__init__.py` — Python implementation module (src/py/vertebra/db/adapters/datafusion/__init__.py).
[^src-db-datafusion-config-py]: `src/py/vertebra/db/adapters/datafusion/config.py` — Python implementation module (src/py/vertebra/db/adapters/datafusion/config.py).
[^src-db-datafusion-driver-py]: `src/py/vertebra/db/adapters/datafusion/driver.py` — Python implementation module (src/py/vertebra/db/adapters/datafusion/driver.py).
[^src-db-init-py]: `src/py/vertebra/db/__init__.py` — Python implementation module (src/py/vertebra/db/__init__.py).
[^src-db-registry-py]: `src/py/vertebra/db/registry.py` — Python implementation module (src/py/vertebra/db/registry.py).
[^src-db-driver-capabilities-py]: `src/py/vertebra/db/driver/capabilities.py` — Python implementation module (src/py/vertebra/db/driver/capabilities.py).
[^src-tests-unit-db-test-pg-dataplane-and-datafusion-py]: `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` — Python implementation module (src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py).

[^src-db-postgres-notify-py]: `src/py/vertebra/db/adapters/postgres/notify.py` — PostgreSQL notification transport and logical message builders.
[^src-extensions-sqlspec-events-py]: `src/py/vertebra/extensions/sqlspec/events.py` — Engine-neutral SQLSpec change-feed event backends.
[^src-tests-unit-db-test-pg-gateway-py]: `src/py/tests/unit/db/test_pg_gateway.py` — CDC spill bounds, key normalization, and storage error coverage.
[^src-tests-unit-extensions-test-data-storage-integrations-py]: `src/py/tests/unit/extensions/test_data_storage_integrations.py` — SQLSpec event backend lifecycle and spill hydration coverage.

[^src-db-spanner-change-stream-py]: `src/py/vertebra/db/adapters/spanner/change_stream.py` — Spanner readers, constructor-bound stream defaults, record parsing, and child-partition scheduling.

[^src-extensions-sqlspec-bigquery-py]: `src/py/vertebra/extensions/sqlspec/bigquery.py` — BigQuery SQLSpec native statement defaults and session execution.
[^src-extensions-sqlspec-spanner-py]: `src/py/vertebra/extensions/sqlspec/spanner.py` — Spanner SQLSpec parser and native parameter defaults.
