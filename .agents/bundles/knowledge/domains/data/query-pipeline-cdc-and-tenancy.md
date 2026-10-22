---
type: Guide
title: "Query Pipelines, Change Feeds, Tenant Predicates & DataFusion"
description: "The PostgreSQL SQL builders in `vertebra.db` (`QueryPipeline`, `RelOverlay`, `TenantPredicate`, `ChangeFeed`) and the `DataFusionEngine` table catalog."
status: stable
tags: [domains, database, postgres, cdc, tenancy]
sources:
  - resource: src/py/vertebra/db/pipeline.py
  - resource: src/py/vertebra/db/cdc.py
  - resource: src/py/vertebra/db/rls.py
  - resource: src/py/vertebra/db/datafusion.py
  - resource: src/py/vertebra/db/__init__.py
  - resource: src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
---

# Query Pipelines, Change Feeds, Tenant Predicates & DataFusion

The `pipeline`, `rls`, `cdc`, and `datafusion` modules in [`vertebra.db`](../../../../../src/py/vertebra/db/__init__.py) generate PostgreSQL SQL text and bind tuples, or keep in-process state. Identifiers interpolated into generated SQL are validated against `^[A-Za-z_][A-Za-z0-9_]*$` (`_validate_identifier`). Drivers and connection configs are covered in [database-drivers-and-dialers](database-drivers-and-dialers.md).

## 1. Single-Round-Trip CTE Pipelines (`pipeline.py`)

| Symbol | Responsibility |
|---|---|
| [`CteStep`](../../../../../src/py/vertebra/db/pipeline.py) | Frozen struct: `name`, `sql`, `params`, `return_json_agg` (default `True`). |
| `RelOverlay` | Read-your-own-writes view inside one statement. `to_cte_sql(name)` validates `name`, `base_table`, `inserted_cte`, `deleted_ids_cte`, and `id_column` via `SqlIdentifier.validate`, then renders `SELECT * FROM base` and excludes `id_column` values in `deleted_ids_cte`, appending `UNION ALL SELECT * FROM inserted_cte`. |
| `QueryPipeline` | Fluent builder: `add_step(name, sql, *, params, return_json_agg)` and `add_overlay(name, overlay)` validate CTE names via `SqlIdentifier.validate` and return `self`. `compile()` returns `(sql, params)`. |
| `QueryPipeline.build_jsonb_object` (static) | Validates column names via `SqlIdentifier.validate` and builds `jsonb_build_object(...)`. Pairs are chunked at `max_pairs_per_call` (1–50, default `chunk_size=50`) and joined with `\|\|`, so wide objects stay under PostgreSQL's 100-argument function limit. An empty column list returns `'{}'::jsonb`. |

Behavior of `compile()`:

- It raises `ValueError` when there are no steps or when any CTE/column identifier fails `SqlIdentifier.validate`.
- It emits `WITH step AS (...), ..., overlay AS (...) SELECT <envelope> AS payload`. The envelope is a JSONB object keyed by step name. Each value is `COALESCE((SELECT jsonb_agg(row_to_json(step)) FROM step), '[]'::jsonb)`, and envelope pairs are chunked 25 per call.
- Steps with `return_json_agg=False` still run as CTEs but are left out of the envelope.
- Step parameter dicts are merged in order, so a later step silently overwrites an earlier step's value for the same key.

## 2. Tenant Isolation (`rls.py`)

| Symbol | Responsibility |
|---|---|
| [`TenantScopeConfig`](../../../../../src/py/vertebra/db/rls.py) | `guc_name="app.tenant_id"`, `tenant_column="tenant_id"`, `parameter_cast="uuid"`. Validated in `__post_init__` via `SqlIdentifier.validate`. |
| `TenantPredicate` | `to_sql_fragment(param_index)` validates `table_alias`, `tenant_column`, and `parameter_cast` via `SqlIdentifier.validate` and renders `alias.tenant_id = $N::uuid`. The cast goes on the parameter, never the column, so the planner can still use an index on `tenant_id`. An empty `table_alias` drops the prefix. |
| `TenantPredicate.build_session_guc_sql` (static) | Returns `("SELECT set_config($1, $2, $3)", (guc_name, tenant_id, is_local))` for a transaction-local GUC that RLS policies can read. |

The intended layering is an explicit predicate for plan quality plus a session GUC for row-level-security policies. Both pieces are SQL fragments only. Nothing in the codebase installs RLS policies.

## 3. Change Feed (`cdc.py`)

| Symbol | Responsibility |
|---|---|
| [`ChangeEvent`](../../../../../src/py/vertebra/db/cdc.py) | `msgspec.Struct(frozen=True, gc=False, kw_only=True, array_like=True)`: `event_id`, `channel`, `operation`, `payload`, `spilled`. |
| `CdcConfig` | `channel="vertebra_events"`, `spill_threshold_bytes=7500`, `spill_table`, `replication_slot_name`, `publication_name`, `max_slot_wal_keep_size_mb=4096`. |
| `ChangeFeed.publish_event(operation, payload)` | Assigns a monotonically increasing `event_id` and serializes with `encode_json_bytes`. Payloads above the threshold are kept in an in-process `_spill_store`, and the wire envelope carries `spilled: true` without the payload. This keeps envelopes under the 8000-byte `NOTIFY` limit. Returns `(wire_envelope, ChangeEvent)` and appends to `_history`. |
| `ChangeFeed.hydrate_notification(envelope)` | Decodes with `decode_json` and restores spilled payloads by `event_id`. An unknown spilled id yields an empty payload. |
| `ChangeFeed.replay_since(last_event_id)` | Returns buffered events newer than the id. |
| `ChangeFeed.build_emit_message_sql` (static) | Returns `SELECT pg_logical_emit_message($1, $2, $3)` with `(transactional, prefix, bytes)`, which emits to the WAL without writing a table. |

Spill storage and history live in process memory and grow without bound. `spill_table`, `replication_slot_name`, `publication_name`, and `max_slot_wal_keep_size_mb` are configuration fields that no code reads.

## 4. DataFusion Catalog (`datafusion.py`)

| Symbol | Responsibility |
|---|---|
| `ARROW_STREAM_MEDIA_TYPE` | `"application/vnd.apache.arrow.stream"`, also exposed as `DataFusionEngine.ARROW_STREAM_MEDIA_TYPE`. |
| `DataFusionMode` | `Literal["in_process", "pgwire_fdw"]`. |
| [`TableRegistration`](../../../../../src/py/vertebra/db/datafusion.py) | `name`, `uri`, `format` (`parquet`, `vortex`, `arrow`, `postgres`, `oracle`). |
| `DataFusionConfig` | `mode`, `target_partitions`, `memory_limit_mb`, `pgwire_host`, `pgwire_port=5434`, `require_mtls_for_pgwire`. |
| `DataFusionEngine` | `register_table`, `tables`, `supports_cloud_run_ingress` (true for `in_process`). `explain_plan(sql)` returns a dict with mode, media type, and the registered table names that appear as substrings of `sql`. `execute_sql(sql)` raises `UnsupportedBackendError` until a native DataFusion runtime is attached. |
