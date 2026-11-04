---
type: Guide
title: "Query Pipelines, Change Feeds, Tenant Predicates & DataFusion"
description: "The PostgreSQL SQL builders in `vertebra.db` (`SyncQueryPipeline` / `AsyncQueryPipeline`, `RelOverlay`, `TenantPredicate`, `ChangeFeed`) and the `DataFusionEngine` table catalog."
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
  - id: src-tests-unit-db-test-pg-dataplane-and-datafusion-py
    resource: src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
    title: "Python implementation module (src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py)"
---

# Query Pipelines, Change Feeds, Tenant Predicates & DataFusion

<!-- truth: start -->
- `ChangeFeed`, `SyncSpannerChangeStreamSource` / `AsyncSpannerChangeStreamSource`, and `TenantPredicate` provide CDC streaming and fail-loud row-level security session scoping across `vertebra.db`.
- `DataFusionEngine` (`vertebra.db.adapters.datafusion`) registers normalized PyArrow, BigQuery, and Spanner tables on a fail-closed `SessionContext` (`ensure_module("datafusion", package="datafusion")`), and all SQL builders validate identifiers via `validate_identifier`.
<!-- truth: end -->

The `pipeline`, `rls`, and `cdc` modules in [`vertebra.db`](../../../../../src/py/vertebra/db/__init__.py)[^src-db-init-py] and the [`vertebra.db.adapters.datafusion`](../../../../../src/py/vertebra/db/adapters/datafusion/__init__.py)[^src-db-datafusion-py] adapter generate PostgreSQL SQL text and bind tuples, or maintain federated analytical session state. Identifiers interpolated into generated SQL are validated against `^[A-Za-z_][A-Za-z0-9_]*$` (`_validate_identifier`). Drivers and connection configs are covered in [database-drivers-and-dialers](database-drivers-and-dialers.md).

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
| [`ChangeEvent`](../../../../../src/py/vertebra/db/cdc.py)[^src-db-cdc-py] | `msgspec.Struct(frozen=True, gc=False, kw_only=True)`: `event_id`, `channel`, `operation`, `payload`, `spilled`. |
| `CdcConfig` | `channel="vertebra_events"`, `spill_threshold_bytes=7500`, `spill_table`, `replication_slot_name`, `publication_name`, `max_slot_wal_keep_size_mb=4096`. |
| `ChangeFeed.publish_event(operation, payload)` | Assigns a monotonically increasing `event_id` and serializes with `encode_json_bytes`. Payloads above the threshold are kept in an in-process `_spill_store`, and the wire envelope carries `spilled: true` without the payload. This keeps envelopes under the 8000-byte `NOTIFY` limit. Returns `(wire_envelope, ChangeEvent)` and appends to `_history`. |
| `ChangeFeed.hydrate_notification(envelope)` | Decodes with `decode_json` and restores spilled payloads by `event_id`. An unknown spilled id yields an empty payload. |
| `ChangeFeed.replay_since(last_event_id)` | Returns buffered events newer than the id. |
| `ChangeFeed.build_emit_message_sql` (static) | Returns `SELECT pg_logical_emit_message($1, $2, $3)` with `(transactional, prefix, bytes)`, which emits to the WAL without writing a table. |
| [`SyncSpannerChangeStreamSource` / `AsyncSpannerChangeStreamSource`](../../../../../src/py/vertebra/db/cdc.py) / `SpannerPartitionRecord` | Queries `READ_json_<stream_name>` (GoogleSQL) or `spanner.read_json_<stream_name>` (Spangres), parses `DataChangeRecord`, `HeartbeatRecord`, and `ChildPartitionsRecord` payloads, fans out child partition readers respecting parent watermark progression, and forwards data changes into `ChangeFeed.publish_event()`. Spangres polls bind `[start_timestamp, end_timestamp, partition_token, heartbeat_milliseconds]` positionally (`$1..$4` → `p1..p4`); GoogleSQL polls bind `@start_ts`, `@end_ts`, `@token`, and `@heartbeat_ms`. |

Spill storage and history live in process memory and grow without bound. `spill_table`, `replication_slot_name`, `publication_name`, and `max_slot_wal_keep_size_mb` are configuration fields that no code reads.

## 4. DataFusion Federation Adapter (`adapters/datafusion/`)

| Symbol | Responsibility |
|---|---|
| `ARROW_STREAM_MEDIA_TYPE` | `"application/vnd.apache.arrow.stream"`, also exposed as `DataFusionEngine.ARROW_STREAM_MEDIA_TYPE`. |
| `DataFusionMode` | `Literal["in_process", "pgwire_fdw"]`. |
| [`TableRegistration`](../../../../../src/py/vertebra/db/adapters/datafusion/config.py)[^src-db-datafusion-config-py] | `name`, `uri`, `format` (`parquet`, `vortex`, `arrow`, `csv`, `ndjson`, `duckdb`, `postgres`, `oracle`, `bigquery`, `spanner`). |
| `DataFusionConfig` | `mode`, `target_partitions`, `memory_limit_mb`, `pgwire_host`, `pgwire_port=5434`, `require_mtls_for_pgwire`. |
| [`DataFusionEngine`](../../../../../src/py/vertebra/db/adapters/datafusion/driver.py)[^src-db-datafusion-driver-py] | Accepts `(config: DataFusionConfig \| None = None, *, ctx: Any = None)`, normalizes `ArrowCursor` / PyArrow inputs into `pyarrow.Table`, resolves `SessionContext` via `ensure_module("datafusion", package="datafusion")` without any SQLite fallback, and propagates `MissingDependencyError` and `SessionContext.sql(...)` errors directly. |

Provenance sources: [^src-tests-unit-db-test-pg-dataplane-and-datafusion-py].

[^src-db-pipeline-py]: `src/py/vertebra/db/pipeline.py` — Python implementation module (src/py/vertebra/db/pipeline.py).
[^src-db-cdc-py]: `src/py/vertebra/db/cdc.py` — Python implementation module (src/py/vertebra/db/cdc.py).
[^src-db-rls-py]: `src/py/vertebra/db/rls.py` — Python implementation module (src/py/vertebra/db/rls.py).
[^src-db-datafusion-py]: `src/py/vertebra/db/adapters/datafusion/__init__.py` — Python implementation module (src/py/vertebra/db/adapters/datafusion/__init__.py).
[^src-db-datafusion-config-py]: `src/py/vertebra/db/adapters/datafusion/config.py` — Python implementation module (src/py/vertebra/db/adapters/datafusion/config.py).
[^src-db-datafusion-driver-py]: `src/py/vertebra/db/adapters/datafusion/driver.py` — Python implementation module (src/py/vertebra/db/adapters/datafusion/driver.py).
[^src-db-init-py]: `src/py/vertebra/db/__init__.py` — Python implementation module (src/py/vertebra/db/__init__.py).
[^src-tests-unit-db-test-pg-dataplane-and-datafusion-py]: `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` — Python implementation module (src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py).
