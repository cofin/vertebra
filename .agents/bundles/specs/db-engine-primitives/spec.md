---
type: Spec
flow_id: db-engine-primitives
title: Engine Primitives Relocation and QueryPipeline Removal
description: Move engine-specific CDC, notification, embedding-pushdown, and DB tool code out of generic modules into vertebra/db/adapters/<engine> primitives and extension tool providers, and delete the QueryPipeline CTE compiler in favor of native driver pipelining.
state: draft
parent_prd: integration-remediation
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 0
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T03:30:00Z
updated_at: 2026-11-03T04:05:00Z
tags: [db, layering, cdc, embeddings, pipeline]
---

# Flow Charter: Engine Primitives Relocation and QueryPipeline Removal

## Charter

After `native-data-platform/db-layout-and-contract` creates `db/adapters/<engine>/`, this flow empties the generic modules of engine code. It also removes the CTE-based `QueryPipeline` now that the native drivers expose `pipeline()`. Real driver I/O, the registry, protocols, and the import gate belong to the native-data-platform PRD.

## Scope

1. **CDC and notifications.**
   - `db/cdc.py` keeps only the generic `ChangeEvent`.
   - The Postgres pieces move to `db/adapters/postgres/notify.py`: `pg_logical_emit_message`, the slot/publication settings from `CDCConfig`, and NOTIFY spill hydration with a bounded spill store and no swallowed exceptions.
   - `SpannerChildPartition`, `SpannerChangeStreamRecord`, and `SpannerChangeStreamSource` move to `db/adapters/spanner/change_stream.py`, implementing `ChangeSource` with separate sync and async readers (no `asyncio.iscoroutine` duck-typing).
   - Oracle TxEventQ and Redis pub/sub use the `NotificationSource` implementations from the native driver chapters.
   - `db/channels.py::DatabaseChannelBackend` is deleted. `queues-tasks-and-channels` builds the Litestar channel backends.
2. **Embedding pushdown.**
   - The SQL builders move to `db/adapters/<engine>/embeddings.py`, implementing `EmbeddingPushdown` with validated identifiers. Sources: `embeddings/_bigquery.py::BigQueryEmbedder`, `embeddings/_spanner.py::SpannerEmbedder`, and `embeddings/_google.py::OracleEmbedder`/`AlloyDbEmbedder`.
   - `embeddings/` keeps the generic `Embedder`, a `PushdownEmbedder` that delegates to an `EmbeddingPushdown`, the remote/local embedders, and the broker.
3. **DB tools.**
   - `db/mcp_tools.py` moves to `extensions/agent/tools/{bigquery,spanner}.py` as `ToolProvider` implementations; `mcp-webmcp-core` defines the protocol.
   - If `mcp-webmcp-core` has not landed, they are plain functions registered by `vertebra.agent`. In both cases `vertebra.db` has no agent/framework imports.
4. **Pipelining.**
   - Delete `db/pipeline.py` (`QueryPipeline`, `CTEStep`, `RelOverlay`), its exports, and its use in `extensions/sqlspec/{bigquery,spanner}.py::*Driver.execute_stack`.
   - Delete `docs/usage/query-pipelines.md`. Rewrite the references in `docs/usage/databases.md`, `docs/usage/extensions.md`, `docs/index.md`, and `docs/reference/index.md` to document `driver.pipeline()`.
5. **RLS.** `db/rls.py` stays generic, with the dialect-specific GUC SQL supplied by each adapter's `core.py`.
6. **Knowledge.** Update `domains/data/query-pipeline-cdc-and-tenancy.md` and `domains/agent/embeddings-and-vectors.md`.

## Public Seams

`vertebra.db.cdc.ChangeEvent`, `vertebra.db.adapters.spanner.change_stream.SpannerChangeStreamSource`, `vertebra.db.adapters.postgres.notify.*`, and `vertebra.embeddings.PushdownEmbedder`.

## Dependencies

integration-remediation `critical-correctness-fixes`. native-data-platform `db-layout-and-contract` and `native-postgres-driver` (for `pipeline()`).

## Acceptance Boundary

- No engine names remain in the engine-agnostic `vertebra/db/*` modules (everything outside `db/adapters/`) apart from the registry tables.
- The import gate passes.
- The docs build (`make docs`).
- `make lint-all` and `make py-test` pass.

## Draft Task Outline

- t01 Postgres notify/CDC relocation.
- t02 Spanner change stream relocation (sync/async readers).
- t03 `DatabaseChannelBackend` removal.
- t04 Embedding pushdown relocation + `PushdownEmbedder`.
- t05 DB tool relocation.
- t06 `QueryPipeline` deletion + docs.
- t07 RLS relocation + knowledge chapters.
