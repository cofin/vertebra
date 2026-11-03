---
type: Spec
flow_id: critical-correctness-fixes
title: Silent Failure and Security Fixes
description: Behavior-only fixes for tenant isolation, MCP exposure and confirmation, SQL injection, store data loss, fake embeddings, registry dialect parsing, CDC parameters, and server loop shutdown.
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
updated_at: 2026-11-03T03:30:00Z
tags: [bugfix, security, regression-tdd]
---

# Flow Charter: Silent Failure and Security Fixes

## Charter

This flow fixes defects that silently lose data, bypass security, or return fabricated results. It keeps public signatures unchanged; restructuring belongs to later chapters. Every task is `regression_tdd`: a failing test in the canonical module test file first, then the fix.

## Scope

1. **Tenant isolation.** `extensions/sqlspec/adapters.py::BaseSyncDriver.apply_tenant_predicate` and `BaseAsyncDriver.apply_tenant_predicate` raise when the RLS GUC `SET` fails instead of suppressing the error.
2. **MCP exposure and confirmation.**
   - `agent/mcp/webmcp.py::build_webmcp_manifest` exposes only tools explicitly tagged for WebMCP. `WebMCPConfig` defaults to opt-in, so untagged tools, including the GCP DB tools, are never browser-exposed.
   - `agent/mcp/service.py::MCPService._handle_tools_call` enforces `requires_confirmation` through `agent/runtime.py::ConfirmationGate`.
3. **`Mcp-Name` bypass.** `agent/mcp/routes.py::MCPHttpHandler.handle_request` rejects a request whose header is present when the body `params.name` is missing or mismatched. `agent/mcp/jsonrpc.py::_invoke_handler` stops unwrapping nested `params`.
4. **DB tool SQL safety.**
   - `db/mcp_tools.py::bigquery_object_table_inspect` validates `table_name` with `vertebra.utils.text.validate_identifier`.
   - `is_mutating_sql` rejects multi-statement input and treats `EXECUTE IMMEDIATE`, `CALL`, `GRANT`, `REVOKE`, `EXPORT DATA`, and `LOAD DATA` as mutating.
5. **Store data loss.**
   - `framework/stores.py::KeyValueStore.delete_all` and `extensions/litestar/store.py::MemoryStore.delete_all` delete only their namespace with `SCAN` + `UNLINK`; no `flushdb`.
   - Sub-second TTLs round up to `px` milliseconds.
   - `expires_in` returns `None` for keys without expiry.
   - `extensions/litestar/config.py::LitestarConfig.swap_memory_channels` and `swap_memory_response_cache` default to `False`.
6. **Fake embeddings.**
   - `embeddings/_spanner.py::SpannerEmbedder._embed_batch_impl` raises on driver errors and on a row/text count mismatch, and drops the `_deterministic_vector` substitution.
   - `embeddings/_local.py::FastEmbedder.__init__`/`GemmaEmbedder.__init__` resolve the real backend on free-threaded builds, and `embeddings/_broker.py::_run_sidecar_worker_main` runs the configured model rather than `_deterministic_hash_backend`.
   - `embeddings/_google.py::OracleEmbedder._embed_batch_impl` raises a typed error on empty results instead of `IndexError`.
   - The `build_*_sql` builders in `embeddings/_bigquery.py` and `embeddings/_spanner.py` validate identifiers.
7. **Registry dialect.** `db/_registry.py::DatabaseRegistry.parse_url` drops the `spanner+postgresql` branch, which emitted the invalid `dialect="postgresql"`. Spangres is selected only through `spangres://` / `spanner+spangres://`, which `SpannerConfig.from_dsn` already parses.
8. **Spangres CDC parameters.** `db/cdc.py::SpannerChangeStreamSource.poll_partition_sync`/`poll_partition_async` pass positional parameters matching `$1..$4` in Spangres mode. Confirm with a failing test first.
9. **Server shutdown.**
   - `server/_loops.py::EventLoopPool.stop` cancels tasks, awaits their cancellation, runs `shutdown_asyncgens`, stops the loop, and closes it only after the thread joins. It raises when `ready.wait` times out.
   - `server/worker.py::_AsgiMountRunner.__call__` takes the batch timeout from configuration (`None` for streaming responses) and cancels the underlying asyncio task on timeout.

## Out of Scope

- Moving `db/mcp_tools.py` (`db-engine-primitives`), the MCP core redesign (Chapter 7), and store replacement (Chapter 5).

## Public Seams

The existing public APIs, with behavior changes only. Changed defaults: `WebMCPConfig` exposure is opt-in, and the `LitestarConfig.swap_memory_*` flags are `False`.

## Dependencies

None. This flow runs first.

## Acceptance Boundary

- Each scope item has a test that fails on `142cc49` and passes after the fix.
- `make lint-all` and `make py-test` pass.

## Draft Task Outline

- t01 Tenant predicate raises.
- t02 MCP exposure opt-in and confirmation enforcement.
- t03 `Mcp-Name` binding and nested-params rejection.
- t04 DB tool identifier validation and statement classification.
- t05 Store namespace delete, TTL rounding, `expires_in` semantics, and swap defaults.
- t06 Embedder error propagation and the real free-threaded backend.
- t07 Registry Spanner scheme and Spangres CDC positional params.
- t08 `EventLoopPool.stop` drain and configurable mount timeout.
