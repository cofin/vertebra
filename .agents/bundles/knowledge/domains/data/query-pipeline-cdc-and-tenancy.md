---
type: Guide
title: "Query Pipelines, Change Feeds, Tenant Predicates & DataFusion"
description: "PostgreSQL query and tenant SQL builders, engine-neutral change feeds, adapter-owned PostgreSQL notifications, Spanner change streams, and the DataFusionEngine table catalog."
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
  - id: src-db-pipeline-py
    resource: src/py/vertebra/db/pipeline.py
    title: "Python implementation module (src/py/vertebra/db/pipeline.py)"
  - id: src-db-cdc-py
    resource: src/py/vertebra/db/cdc.py
    title: "Python implementation module (src/py/vertebra/db/cdc.py)"
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

# Query Pipelines, Change Feeds, Tenant Predicates & DataFusion

<!-- truth: start -->
- `ChangeFeed` provides engine-neutral event envelopes, bounded spill hydration, replay, and Parquet archival; PostgreSQL notification SQL lives in `vertebra.db.adapters.postgres.notify`.
- `SyncSpannerChangeStreamSource` / `AsyncSpannerChangeStreamSource` remain in `vertebra.db.cdc`; `TenantPredicate` provides fail-loud row-level security session scoping.
- `DataFusionEngine` (`vertebra.db.adapters.datafusion`) registers normalized PyArrow, BigQuery, and Spanner tables on a fail-closed `SessionContext` (`ensure_module("datafusion", package="datafusion")`), and all SQL builders validate identifiers via `validate_identifier`.
<!-- truth: end -->

The `pipeline` and `rls` modules in [`vertebra.db`](../../../../../src/py/vertebra/db/__init__.py)[^src-db-init-py] generate PostgreSQL SQL text and bind tuples. The `cdc` module manages change-feed envelopes and Spanner change streams, [`postgres.notify`](../../../../../src/py/vertebra/db/adapters/postgres/notify.py)[^src-db-postgres-notify-py] owns PostgreSQL notification SQL, and the [`vertebra.db.adapters.datafusion`](../../../../../src/py/vertebra/db/adapters/datafusion/__init__.py)[^src-db-datafusion-py] adapter maintains federated analytical session state. Identifiers interpolated into generated SQL are validated against `^[A-Za-z_][A-Za-z0-9_]*$` (`_validate_identifier`). Drivers and connection configs are covered in [database-drivers-and-dialers](database-drivers-and-dialers.md).

## 1. Single-Round-Trip CTE Pipelines (`pipeline.py`)

| Symbol | Responsibility |
|---|---|
| [`CTEStep`](../../../../../src/py/vertebra/db/pipeline.py)[^src-db-pipeline-py] | Frozen struct: `name`, `sql`, `params`, `return_json_agg` (default `True`). |
| `RelOverlay` | Read-your-own-writes view inside one statement. `to_cte_sql(name)` validates `name`, `base_table`, `inserted_cte`, `deleted_ids_cte`, and `id_column` via `SqlIdentifier.validate`, then renders `SELECT * FROM base` and excludes `id_column` values in `deleted_ids_cte`, appending `UNION ALL SELECT * FROM inserted_cte`. |
| `SyncQueryPipeline` / `AsyncQueryPipeline` | Fluent builder (`dialect: Literal["postgres", "googlesql"] = "postgres"`): `add_step(name, sql, *, params, return_json_agg)` and `add_overlay(name, overlay)` validate CTE names via `SqlIdentifier.validate` and return `self`. `compile()` returns `(sql, params)`. |
| `SyncQueryPipeline.build_jsonb_object` / `AsyncQueryPipeline.build_jsonb_object` (static) | Validates column names via `SqlIdentifier.validate` and builds `jsonb_build_object(...)` on `postgres` or `TO_JSON(STRUCT(...))` on `googlesql`. Pairs are chunked at `max_pairs_per_call` (1–50, default `50`) and joined with `\|\|`, so wide objects stay under PostgreSQL's 100-argument function limit. An empty column list returns `'{}'::jsonb` (`postgres`) or `JSON '{}'` (`googlesql`). |

Behavior of `compile()`:

- It raises `ValueError` when there are no steps or when any CTE/column identifier fails `SqlIdentifier.validate`.
- It emits `WITH step AS (...), ..., overlay AS (...) SELECT <envelope> AS payload`. On `postgresql`, each value is `COALESCE((SELECT jsonb_agg(row_to_json(step)) FROM step), '[]'::jsonb)` inside chunked `jsonb_build_object` calls; on `googlesql` (BigQuery and Cloud Spanner), each value is `COALESCE((SELECT TO_JSON(ARRAY_AGG( step)) FROM step), JSON '[]')` inside `TO_JSON_STRING(JSON_OBJECT(...))`.
- Steps with `return_json_agg=False` still run as CTEs but are left out of the envelope.
- Step parameter dicts are merged in order, so a later step silently overwrites an earlier step's value for the same key.

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
| [`SyncSpannerChangeStreamSource` / `AsyncSpannerChangeStreamSource`](../../../../../src/py/vertebra/db/cdc.py) / `SpannerChildPartition` / `SpannerChangeStreamRecord` | Implements the [`SyncChangeSource` / `AsyncChangeSource`](../../../../../src/py/vertebra/db/driver/capabilities.py)[^src-db-driver-capabilities-py] capability protocols (`stream`), querying `READ_<stream_name>` in GoogleSQL (`spanner.read_json_<stream_name>` is Spangres-only), parsing `DataChangeRecord`, `HeartbeatRecord`, and `ChildPartitionsRecord` payloads into `SpannerChangeStreamRecord` and `SpannerChildPartition` structs, fanning out child partition readers respecting parent watermark progression, and forwarding data changes into `ChangeFeed.publish_event()`. Spangres polls bind `[start_timestamp, end_timestamp, partition_token, heartbeat_milliseconds]` positionally (`$1..$4` → `p1..p4`); GoogleSQL polls bind `@start_ts`, `@end_ts`, `@token`, and `@heartbeat_ms`. |

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

[^src-db-pipeline-py]: `src/py/vertebra/db/pipeline.py` — Python implementation module (src/py/vertebra/db/pipeline.py).
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
