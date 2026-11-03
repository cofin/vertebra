---
type: Spec
flow_id: critical-correctness-fixes
title: Silent Failure and Security Fixes
description: Behavior-only fixes for tenant isolation, MCP exposure and confirmation, Mcp-Name binding, DB tool SQL safety, store data loss, fabricated embeddings, Spanner URL and CDC parameters, and server loop shutdown.
state: active
parent_prd: integration-remediation
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 2
plan_commit: 24e2931
state_revision: 1
current_task: t01
last_verified_checkpoint: null
created_at: 2026-11-03T03:30:00Z
updated_at: 2026-11-03T16:00:00Z
tags: [bugfix, security, regression-tdd]
---

# Flow: Silent Failure and Security Fixes

## 1. Specification & Context

### 1.1 Problem Statement & Objectives

The `main @ 142cc49` audit ([audit-findings_20261103.md](../integration-remediation/research/audit-findings_20261103.md) sections 1.4, 2.2, 3.2, 3.3, 3.5, 5.2) found code paths that lose data, skip security checks, or return fabricated results without raising:

- `extensions/sqlspec/adapters.py` swallows the tenant RLS `SET` failure, so a session runs without tenant isolation.
- WebMCP publishes every tool, including the GCP database tools, unless a tool opts out. `MCPService` never checks `Tool.requires_confirmation`, and `ConfirmationGate.approve` records a token before checking that the ticket exists and belongs to the caller, so a failed approval still marks the token approved.
- An `Mcp-Name` header is not compared when the body omits `params.name` or when the body is a JSON-RPC batch, and the JSON-RPC router unwraps a nested `params` object. Together they let a request run a tool that the header check never saw.
- `db/mcp_tools.py` interpolates `table_name` into SQL, and `is_mutating_sql` reads only the first keyword, so `SELECT 1; DELETE ...`, `CALL`, `GRANT`, `EXPORT DATA`, and `LOAD DATA` skip the confirmation gate.
- `KeyValueStore.delete_all` / Litestar `MemoryStore.delete_all` / Starlette `CacheStore.delete_all` run `FLUSHDB`, which deletes every key in the Redis/Valkey database. Sub-second TTLs send `EX 0`, which Redis rejects. `expires_in` returns `-1` for keys without expiry, where Litestar's `Store` contract requires `None`. `LitestarConfig` swaps Litestar's channels backend and response cache by default.
- `SpannerEmbedder` replaces driver errors and missing rows with SHA-256 noise vectors, and `BigQueryEmbedder` returns short or empty results without error. Both generate builders order only the input subquery, so result rows can come back out of text order. On free-threaded builds, `FastEmbedder`/`GemmaEmbedder` send inference to a subprocess that only runs a hash function. `OracleEmbedder` raises `IndexError` on an empty result. The BigQuery and Spanner `build_*_sql` builders interpolate identifiers without validation.
- `DatabaseRegistry.parse_url` rejects `spangres://` even though `SpannerConfig.from_dsn` accepts it, and it carries a dead `spanner+postgresql` branch.
- `SpannerChangeStreamSource.poll_partition_*` binds named parameters to the Spangres query, which uses positional `$1..$4`.
- `EventLoopPool.stop` cancels tasks and stops the loop in the same callback, so cancellation handlers and async-generator finalizers never run, and it closes loops whose threads may still be running. Startup ignores a shard that never signals readiness. `server/worker.py::_AsgiMountRunner.__call__`, the buffered ASGI mount path called only by `server/_dispatch.py::dispatch_batch` with `async_mode=False` (the production worker uses `async_mode=True` and `spawn_batch`, so streaming never goes through it), uses a hardcoded 10 s timeout and leaves the timed-out asyncio task running.

Each fix changes behavior inside the existing module and keeps public signatures, except for the additive parameters listed in §2.2. Every task is `regression_tdd`: a failing test in the canonical module test file first, then the fix.

**Non-Goals**

- Moving `db/mcp_tools.py` out of `vertebra.db` (`db-engine-primitives`), the MCP core redesign, single-use confirmation tokens, the WebMCP TypeScript bridge, Origin defaults, HTTP status-code conformance, and removing JSON-RPC batch support (`mcp-webmcp-core`).
- A WebMCP exposure path for tools registered from a route `opt["mcp_tool"]` entry (`MCPRegistry.register_route_handler` `opt_tool` branch, `AgentPlugin` `opt["mcp_tool"]` handling). Those tools carry no `_mcp_tool_meta`, so after this flow they are never browser-exposed until `mcp-webmcp-core` adds a tag for them.
- The hardcoded 10 s wait in `server/_dispatch.py::dispatch_batch`'s buffered `async_native` branch (`runtime-structure-and-throughput`, which owns the dispatch restructure).
- Deduplicating `KeyValueStore`, Litestar `MemoryStore`, and Starlette `CacheStore`, bounding the local `_entries` cache, cross-process cache invalidation on Redis, and Redis Cluster-aware `SCAN` (`queues-tasks-and-channels`).
- `extensions/alchemy/cache.py::SQLAlchemyCacheBackend.clear` / `clear_async` also call `FLUSHDB` (found during planning). Scoping them requires aligning the backend's driver keys with Advanced Alchemy's region `key_prefix` (`create_cache_config(key_prefix="vtb:aa:")`), which the `sqlalchemy-and-django-integration` chapter owns; that charter must add it.
- Moving embedders into engine packages, sync ONNX inference on the event loop, and Vertex transport (`db-engine-primitives`, `runtime-structure-and-throughput`).
- `ChangeFeed` spill bounds, `hydrate_notification` exception handling, and `_next_id` thread safety (`db-engine-primitives`).
- `execute_stack` CTE emulation (removed with `QueryPipeline` in `db-engine-primitives`).
- Fastpath body caps and `resolve_loop_backend` restructuring (`runtime-structure-and-throughput`).

### 1.2 Upstream Execution Baseline

- **Parent PRD**: [integration-remediation](../integration-remediation/spec.md). Chapter 1. No dependencies; this flow runs first.
- **Baseline**: `main @ 142cc49`. The planning branch `chore/integration-remediation-plan` has the same code tree.
- **Branch**: implement on `fix/critical-correctness` created from `main`:

  ```bash
  git fetch origin
  git switch -c fix/critical-correctness origin/main
  make dev
  ```

- Each task is one signed commit (`git commit -S`). Commits stay local until the user asks for delivery.

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Tenant RLS | `src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver.apply_tenant_predicate`, `::BaseAsyncDriver.apply_tenant_predicate`, `src/py/vertebra/db/rls.py::TenantPredicate.build_session_guc_sql` | Public driver method. Local-substitutable: SQLite sessions from `SQLiteSyncConfig`/`SQLiteAsyncConfig` reject `set_config`, which reproduces a failing `SET`. |
| Spanner URL | `src/py/vertebra/db/_registry.py::DatabaseRegistry.parse_url`, `src/py/vertebra/db/spanner/config.py::SpannerConfig.from_dsn` | In-process parsing. Tested directly. |
| Spanner CDC | `src/py/vertebra/db/cdc.py::SpannerChangeStreamSource.build_read_query`, `::poll_partition_sync`, `::poll_partition_async`, `src/py/vertebra/db/spanner/core.py` (sequence params map to `p1..pN`) | Remote-owned driver. Tests use a recording driver stand-in with `select(sql, params)`. |
| WebMCP exposure | `src/py/vertebra/agent/mcp/webmcp.py::WebMCPConfig`, `::build_webmcp_manifest`, `src/py/vertebra/agent/mcp/controllers.py::mcp_tool` | In-process manifest projection. Tested directly. |
| MCP confirmation | `src/py/vertebra/agent/mcp/service.py::MCPService.__init__`, `::handle_tool_call`, `::_handle_tools_call`, `src/py/vertebra/agent/runtime.py::ConfirmationGate`, `src/py/vertebra/agent/plugin.py::AgentPlugin.__init__` | In-process service. Tested through `MCPService.handle_tool_call` and `AgentPlugin.chat_controller.decide_confirmation`. |
| `Mcp-Name` binding | `src/py/vertebra/agent/mcp/routes.py::MCPHttpHandler.handle_request`, `src/py/vertebra/agent/core/jsonrpc.py::JSONRPCRouter._invoke_handler` | In-process HTTP handler and router. Tested through `MCPHttpHandler.handle_request` and `JSONRPCRouter.dispatch`. |
| DB tool SQL | `src/py/vertebra/db/mcp_tools.py::is_mutating_sql`, `::_extract_leading_verb`, `::register_gcp_database_tools` (inner `bigquery_object_table_inspect`, `spanner_query`), `src/py/vertebra/utils/text.py::validate_identifier` | In-process classification. Tool tests reuse `_StubBigQueryDriver` and `FakeSpannerClient`. |
| Stores | `src/py/vertebra/framework/stores.py::KeyValueStore`, `src/py/vertebra/extensions/litestar/store.py::MemoryStore`, `src/py/vertebra/extensions/starlette/store.py::CacheStore`, `src/py/vertebra/db/redis/driver.py::SyncRedisDriver`, `::AsyncRedisDriver`, `src/py/vertebra/extensions/litestar/config.py::LitestarConfig`, `src/py/vertebra/extensions/litestar/plugin.py::LitestarPlugin._finalize_app_acceleration` | Local-substitutable: the Redis driver's `memory://` mode. |
| Remote embedders | `src/py/vertebra/embeddings/_spanner.py::SpannerEmbedder`, `src/py/vertebra/embeddings/_google.py::OracleEmbedder._embed_batch_impl`, `src/py/vertebra/embeddings/_bigquery.py::BigQueryEmbedder`, `src/py/vertebra/embeddings/_base.py` | Remote-owned drivers. Tests use stub drivers with `select`/`fetch_all`/`execute`. |
| Local embedders | `src/py/vertebra/embeddings/_broker.py::EmbeddingBroker`, `::_run_sidecar_worker_main`, `src/py/vertebra/embeddings/_local.py::FastEmbedder`, `::GemmaEmbedder` | `fastembed` is an external boundary: in-process tests substitute a fake `fastembed` module, and sidecar tests use an importable test backend factory. |
| Loop shutdown | `src/py/vertebra/server/_loops.py::EventLoopPool.__init__`, `::stop`, `::schedule_batch` | In-process threads and loops. Tested directly. |
| Mount timeout | `src/py/vertebra/server/worker.py::_AsgiMountRunner`, `::_instantiate_mount_runners`, `::_resolve_configured_interface` (existing env-resolver pattern) | In-process. Tested with a two-shard `EventLoopPool` and an ASGI app that never completes. |

### 1.4 Relevant Patterns, Domain Terms & Knowledge Chapters

- [Product Guidelines](../../product/product-guidelines.md): msgspec value objects, no aliases, no inline comments, module and class layout.
- [Module & Class Layout](../../knowledge/patterns/python/module-and-class-layout.md), [Serialization & Utils](../../knowledge/patterns/python/serialization-and-utils.md), [Hot Path & Async Dispatch](../../knowledge/patterns/python/hot-path-and-async-dispatch.md).
- [Polyglot Test Matrix](../../knowledge/testing/polyglot-test-matrix.md): canonical module test files, `@pytest.mark.anyio`, strong assertions.
- Domain chapters this flow corrects: [query-pipeline-cdc-and-tenancy](../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [database-drivers-and-dialers](../../knowledge/domains/data/database-drivers-and-dialers.md), [mcp-and-a2a-protocol-harness](../../knowledge/domains/agent/mcp-and-a2a-protocol-harness.md), [embeddings-and-vectors](../../knowledge/domains/agent/embeddings-and-vectors.md), [litestar](../../knowledge/domains/extensions/litestar.md), [platform-and-lifecycle](../../knowledge/architecture/runtime/platform-and-lifecycle.md).
- Skills: [python-framework](../../../skills/python-framework/SKILL.md), [review-tests](../../../skills/review-tests/SKILL.md).
- **Terms**: *tenant GUC* is the `app.tenant_id` session setting written by `set_config`. *Confirmation ticket* is a `ConfirmationTicket` issued by `ConfirmationGate.request_confirmation`. *Key prefix* is the store namespace that every Redis/Valkey key written by a store starts with (`<key_prefix>:<key>`). *Sidecar* is the quarantined embedding subprocess started by `EmbeddingBroker` on free-threaded builds. *Backend factory* is an importable `module:qualname` callable that builds an inference callable.

## 2. Requirements & Architecture

### 2.1 Functional Requirements

- **R1**: `apply_tenant_predicate` (sync and async) propagates the driver exception raised by the tenant GUC `SET` and returns no SQL fragment when the `SET` fails.
- **R2**: `DatabaseRegistry.parse_url` maps `spangres://` to `SpannerConfig` with `dialect="spangres"` and has no `spanner+postgresql` branch. `spanner+postgresql://` raises `ImproperConfigurationError`, as it does at `142cc49`.
- **R3**: `SpannerChangeStreamSource.poll_partition_sync`/`poll_partition_async` pass the positional list `[start_timestamp, end_timestamp, partition_token, heartbeat_milliseconds]` in Spangres mode and keep the named mapping in GoogleSQL mode.
- **R4**: `build_webmcp_manifest` includes a tool only when its `@mcp_tool` metadata has `webmcp=True`. `mcp_tool(webmcp=...)` defaults to `False`, and `WebMCPConfig.include_tagged_only` is removed. Untagged `Tool` objects, including the GCP database tools and tools registered from `opt["mcp_tool"]` route entries, are never in the manifest (opt-dict tools have no exposure path until `mcp-webmcp-core`).
- **R5**: `MCPService.handle_tool_call` refuses to invoke a tool with `requires_confirmation=True` unless `params["_meta"]["vertebra/confirmationToken"]` names a `ConfirmationTicket` approved for that tool, caller, and arguments (`status == "approved"`, owner `None` or equal to the caller, same `tool_name`, equal `arguments`). Otherwise it issues a ticket and raises `JSONRPCErrorException` with code `CONFIRMATION_REQUIRED` (`-32003`) and `data={"confirmationToken": <token>, "tool": <name>}`. `ConfirmationGate.approve` and `reject` return `False` without changing state when the ticket is unknown or owned by another caller. `AgentPlugin` gives its `MCPService` the runtime's `ConfirmationGate`, so `/agent/confirmations/{token}` approves MCP tickets.
- **R6**: `MCPHttpHandler.handle_request` returns a `HEADER_MISMATCH` error when an `Mcp-Name` header is present for a method in `MCP_NAME_FIELDS` and the body field is missing or different, and when an `Mcp-Method` or `Mcp-Name` header accompanies a JSON array (batch) body. `JSONRPCRouter._invoke_handler` passes a dict `params` to handlers that declare a `params` parameter as `params=<dict>`, never unpacks a nested `params` key, and never retries a handler that raised `TypeError`.
- **R7**: `bigquery_object_table_inspect` validates `table_name` with `validate_identifier(..., allow_hyphens=True)` before building SQL. `is_mutating_sql` raises `PermissionDeniedError` when a semicolon appears before the trailing terminator (so no second statement can run) and treats `EXECUTE`, `CALL`, `GRANT`, `REVOKE`, `EXPORT`, and `LOAD` as mutating. `spanner_query` applies the same rules and sends `GRANT`/`REVOKE` through `update_ddl`.
- **R8**: `KeyValueStore`, Litestar `MemoryStore`, and Starlette `CacheStore` write every driver key as `<key_prefix>:<key>` (default `key_prefix="vertebra"`). `delete_all` removes only that prefix with `SCAN MATCH <key_prefix>:*` and `UNLINK`. `set` sends the TTL as `PX` milliseconds rounded up and raises `ValueError` for a non-positive `expires_in`. `KeyValueStore.expires_in` and `MemoryStore.expires_in` return `None` for missing keys and for keys without expiry.
- **R9**: `LitestarConfig.swap_memory_channels` and `LitestarConfig.swap_memory_response_cache` default to `False`.
- **R10**: `SpannerEmbedder` propagates driver errors, raises `EmbeddingError` when the row count differs from the text count or a row has no vector, and has no hash-vector substitution. `BigQueryEmbedder` raises `EmbeddingError` on a row-count mismatch or an empty vector. Both generate builders select the vector as column 0, pass the input offset `idx` through the model function as the second column, and end with `ORDER BY idx`, so rows match texts by position. `OracleEmbedder` raises `EmbeddingError` on an empty result. `BigQueryEmbedder` and `SpannerEmbedder` builders validate the model endpoint, table, and column identifiers and reject unknown distance types with `ValueError`.
- **R11**: `EmbeddingBroker` requires a `backend_fn` or a `BackendFactory`. The sidecar builds and runs the configured factory instead of a hash backend. `FastEmbedder` and `GemmaEmbedder` give their broker the fastembed `BackendFactory`, so free-threaded builds run the real model in the sidecar, and `FastEmbedder` no longer builds a discarded `TextEmbedding` at construction.
- **R12**: `EventLoopPool.stop` cancels every pending task on each shard, waits for the cancellations, runs `shutdown_asyncgens`, stops the loop, joins the thread, and closes a loop only after its thread has exited. `EventLoopPool.__init__` raises `RuntimeError` when a shard does not signal readiness within the start timeout. `schedule_batch` cancels the asyncio task when its returned future is cancelled.
- **R13**: `_AsgiMountRunner` (the buffered ASGI mount path reached through `dispatch_batch(async_mode=False)`) takes `batch_timeout: float | None` with a 30 s default. `_instantiate_mount_runners` reads it from `VERTEBRA_ASGI_BATCH_TIMEOUT` (seconds; `0` or `none` disables; negative or non-numeric raises `ImproperConfigurationError`). On timeout the runner cancels the asyncio task and returns `504 Gateway Timeout` for that request.

### 2.2 Public Interfaces, Deep Modules & Seam Contracts

```python
# vertebra/agent/mcp/service.py
CONFIRMATION_REQUIRED: int = -32003
CONFIRMATION_TOKEN_META_KEY: str = "vertebra/confirmationToken"

class MCPService:
    def __init__(self, config=None, registry=None, telemetry=None, stream_broker=None,
                 task_store=None, skill_catalog=None,
                 confirmation_gate: ConfirmationGate | None = None) -> None: ...
    confirmation_gate: ConfirmationGate

# vertebra/agent/runtime.py
class ConfirmationGate:
    def approve(self, token: str, *, owner_id: str | None = None) -> bool: ...
    def reject(self, token: str, *, owner_id: str | None = None, reason: str = "") -> bool: ...
    # Both return False and leave the gate unchanged when the ticket is unknown or owned by
    # another caller; True after recording the decision on the ticket.

# vertebra/agent/mcp/controllers.py
def mcp_tool(name=None, description=None, *, requires_confirmation=False,
             timeout_seconds=None, annotations=None, webmcp: bool = False): ...

# vertebra/agent/mcp/webmcp.py
class WebMCPConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    enabled: bool = True
    manifest_path: str = "/.well-known/webmcp.json"
    mcp_endpoint: str = "/mcp"

# vertebra/framework/stores.py and vertebra/extensions/litestar/store.py
class KeyValueStore / MemoryStore:
    def __init__(self, *, driver=None, key_prefix: str = "vertebra") -> None: ...
    @classmethod
    def from_valkey(cls, config_or_url, *, key_prefix: str = "vertebra") -> Self: ...
    @classmethod
    def from_redis(cls, config_or_url, *, key_prefix: str = "vertebra") -> Self: ...
    @property
    def key_prefix(self) -> str: ...

# vertebra/extensions/starlette/store.py
class CacheStore:
    def __init__(self, *, async_driver=None, sync_driver=None, key_prefix: str = "vertebra") -> None: ...
    @classmethod
    def from_valkey(cls, config_or_url, *, include_sync: bool = True, key_prefix: str = "vertebra") -> Self: ...
    @classmethod
    def from_redis(cls, config_or_url, *, include_sync: bool = True, key_prefix: str = "vertebra") -> Self: ...
    @property
    def key_prefix(self) -> str: ...

# vertebra/db/redis/driver.py (SyncRedisDriver sync, AsyncRedisDriver async)
def set(self, key, value, *, ex: int | None = None, px: int | None = None) -> None: ...
def scan(self, cursor: int = 0, *, match: str | None = None, count: int = 500) -> tuple[int, list[str]]: ...
def unlink(self, *keys: str) -> int: ...

# vertebra/embeddings/_base.py (exported from vertebra.embeddings)
class EmbeddingError(VertebraError, RuntimeError): ...

# vertebra/embeddings/_broker.py (exported from vertebra.embeddings)
class BackendFactory(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    target: str
    options: dict[str, Any] = msgspec.field(default_factory=dict)
    def build(self) -> BackendInferenceCallable: ...

class EmbeddingBroker:
    def __init__(self, backend_fn: BackendInferenceCallable | None = None, *, dimensions=768,
                 config=None, backend_factory: BackendFactory | None = None) -> None: ...
    # backend_fn is optional; at least one of backend_fn / backend_factory is required.
    @property
    def backend_factory(self) -> BackendFactory | None: ...

# vertebra/embeddings/_local.py
class FastEmbedder:
    @staticmethod
    def build_backend(model_name: str, *, cache_dir: str | None = None,
                      specific_model_path: str | None = None,
                      threads: int | None = None) -> BackendInferenceCallable: ...

# vertebra/server/worker.py
_DEFAULT_ASGI_BATCH_TIMEOUT_SECONDS: float = 30.0

class _AsgiMountRunner:
    def __init__(self, app, pool, *, mount_prefix="", lifespan_state=None,
                 batch_timeout: float | None = _DEFAULT_ASGI_BATCH_TIMEOUT_SECONDS) -> None: ...
    @property
    def batch_timeout(self) -> float | None: ...

def _resolve_asgi_batch_timeout() -> float | None: ...
# Unset/empty -> 30.0; "0" or "none" (any case) -> None; positive number -> value;
# negative or non-numeric -> ImproperConfigurationError.
```

Error modes:

| Seam | Condition | Raised / Returned |
| --- | --- | --- |
| `apply_tenant_predicate` | GUC `SET` fails | Driver exception unchanged (SQLite: `sqlite3.OperationalError`) |
| `DatabaseRegistry.parse_url` | `spanner+postgresql://` | `ImproperConfigurationError` from `SpannerConfig.from_dsn` |
| `MCPService.handle_tool_call` | confirmation missing, unapproved, other owner, other tool, or other arguments | `JSONRPCErrorException(code=-32003)` |
| `ConfirmationGate.approve` / `reject` | unknown ticket or other owner | `False`, gate state unchanged |
| `MCPHttpHandler.handle_request` | header present, body field missing or different, or batch body | HTTP 200 with JSON-RPC error `HEADER_MISMATCH` (status codes change in `mcp-webmcp-core`) |
| `is_mutating_sql` | semicolon before the trailing terminator | `PermissionDeniedError("Multi-statement SQL is not allowed; ...")` |
| `bigquery_object_table_inspect` | invalid `table_name` | `ValueError` from `validate_identifier` |
| Store `set` | `expires_in <= 0` | `ValueError("expires_in must be a positive duration")` |
| Store `__init__` | invalid `key_prefix` | `ValueError` from `validate_identifier` |
| Redis `set` | both `ex` and `px` | `ValueError("set() accepts ex or px, not both")` |
| `SpannerEmbedder` / `BigQueryEmbedder` | row count mismatch / row without vector | `EmbeddingError` |
| `OracleEmbedder` | empty result | `EmbeddingError` |
| `EmbeddingBroker.__init__` | neither `backend_fn` nor `backend_factory` | `ImproperConfigurationError` |
| `EventLoopPool.__init__` | shard not ready in time | `RuntimeError` |
| `_instantiate_mount_runners` | negative or non-numeric `VERTEBRA_ASGI_BATCH_TIMEOUT` | `ImproperConfigurationError` |
| `_AsgiMountRunner.__call__` | request exceeds `batch_timeout` | `AsgiResponse(status=504, body=b"Gateway Timeout")`, task cancelled |

### 2.3 Non-Functional & Safety Constraints

- Python code rules (repeat in every worksheet): `msgspec.Struct(frozen=True, gc=False)` instead of dataclasses; PEP 585/604/695 types; Google-style docstrings; zero inline `#` comments; top-level imports only (except `TYPE_CHECKING` blocks and optional-dependency helpers); class layout attributes → `__init__` → properties → public methods → private methods; JSON through `vertebra.utils.serializers`; `@pytest.mark.parametrize` instead of loops over cases; no compatibility aliases, shims, or `try/except` fallbacks added for test seeds.
- Tests go into the existing canonical test modules named in each worksheet. No `test_*remediation*` / `test_*audit*` files. No finding IDs in code, tests, or commits.
- Gates: `make lint` before each commit; phase checkpoint `make lint-all` and `make py-test` (which runs `make dev`).
- Changelog entries go under `## 0.3.0` in `docs/changelog/index.md` (`### Changed` for default or contract changes, `### Fixed` for defects), without PR links. The PR link is added at delivery.

### 2.4 Acceptance Scenarios (Given / When / Then)

1. **Given** a SQLite sqlspec session, **when** `apply_tenant_predicate` runs, **then** `sqlite3.OperationalError` matching `set_config` propagates.
2. **Given** `spangres://proj/inst/db`, **when** `DatabaseRegistry.parse_url` runs, **then** it returns a `SpannerConfig` with `dialect == "spangres"`.
3. **Given** a Spangres change stream source, **when** `poll_partition_sync(start_timestamp="2026-11-02T00:00:00Z", partition_token="tok-1")` runs, **then** the driver receives `["2026-11-02T00:00:00Z", None, "tok-1", 10000]`.
4. **Given** one `@mcp_tool(webmcp=True)` tool and one untagged tool, **when** the manifest is built, **then** it lists only the tagged tool.
5. **Given** a tool with `requires_confirmation=True`, **when** it is called without a token, **then** the call raises code `-32003` and the tool does not run; after the returned token is approved, a call with a different tool or different arguments still raises `-32003`, and the original call with `_meta["vertebra/confirmationToken"]` runs it once.
6. **Given** `Mcp-Name: other` and a body whose `params` only nests `{"params": {"name": "calc_add"}}`, or a JSON array body, **when** the handler runs, **then** it returns `HEADER_MISMATCH` and the tool does not run.
7. **Given** `SELECT 1; DELETE FROM orders`, **when** `is_mutating_sql` runs, **then** it raises `PermissionDeniedError`; `CALL p()`, `GRANT ...`, `EXPORT DATA ...`, `LOAD DATA ...`, and `EXECUTE IMMEDIATE ...` return `True`.
8. **Given** a `memory://` driver holding `other:keep` and a store holding `a`, **when** `delete_all` runs, **then** `other:keep` survives and `vertebra:a` is gone.
9. **Given** `expires_in=timedelta(milliseconds=250)`, **when** the store sets a key, **then** the driver PTTL is between 1 and 250 ms.
10. **Given** a Litestar app with `ChannelsPlugin(MemoryChannelsBackend)` and the default `LitestarConfig`, **when** the plugin finalizes, **then** the backend is still `MemoryChannelsBackend`.
11. **Given** a Spanner driver whose `select` raises `RuntimeError("boom")`, **when** `SpannerEmbedder.embed_many` runs, **then** `RuntimeError("boom")` propagates.
12. **Given** a broker whose `BackendFactory` targets a test backend that encodes length, **when** it runs in the sidecar, **then** the vectors match that backend (`[0.9701425, 0.2425356, 0, 0]` for `"ab"` with `scale=2.0`).
13. **Given** a pool task awaiting `asyncio.sleep(3600)` with a `CancelledError` handler, **when** `EventLoopPool.stop()` returns, **then** the handler ran and every loop is closed.
14. **Given** `batch_timeout=0.05` and an ASGI app that never responds, **when** the mount runner runs, **then** it returns status 504 and the app observes `CancelledError`.
15. **Given** a ticket owned by `default:alice`, **when** `ConfirmationGate.approve(token, owner_id="default:mallory")` runs, **then** it returns `False`, the ticket stays `pending`, and alice's call with that token still raises `-32003`.

### 2.5 Alternatives Considered

- **Tenant failure**: wrapping the driver error in a new `TenantIsolationError` was rejected. It adds public surface, and callers already handle the driver's own exception type. Propagating the original exception is the smallest correct change.
- **WebMCP opt-in**: keeping `include_tagged_only` with a `True` default was rejected because it leaves a switch that re-exposes every untagged tool. Gating on `WebMCPConfig.enabled=False` alone was rejected because it does not stop an enabled manifest from listing the GCP database tools. Per-tool opt-in (`webmcp=True`) places the decision with the tool author, where the risk is known.
- **Confirmation transport**: a new top-level `confirmationToken` param was rejected because MCP reserves top-level `tools/call` params; `_meta` with a prefixed key is the extension point MCP defines. Returning `isError` tool content was rejected because the tool did not run; a JSON-RPC error tells the client to obtain approval. Relying on `ConfirmationGate.is_approved` alone was rejected because `approve` recorded tokens before its ticket and owner checks; the gate is fixed at the root, and the service also checks ticket status, owner, tool name, and arguments so an approval for one call cannot authorize another.
- **Nested params and batches**: stripping a nested `params` key in the router was rejected because it still guesses at intent. Passing the dict as `params=` to handlers that declare `params` removes the guess and the `TypeError` retry chain that hid handler errors. Validating `Mcp-Name` against every batch element was rejected because one header cannot name several calls; MCP routing headers on a batch body are refused outright.
- **Statement classification**: literal-aware masking was rejected because GoogleSQL uses backslash escapes while Spangres uses standard strings, `E''` strings, and dollar quoting, so one masking pass can hide a separator in one dialect. `sqlglot` was rejected because it is not a core dependency and still needs a dialect choice. Rejecting every semicolon before the trailing terminator fails closed in every dialect; a value containing `;` goes in a query parameter.
- **Store namespace**: tracking written keys in-process was rejected because it misses keys written by other workers. Litestar's own `RedisStore` uses a key prefix with `SCAN` + delete, so this follows framework precedent. `key_prefix` is used instead of `namespace` because `KeyValueStore.namespace()` already exists as a method returning `NamespacedStore`.
- **Embedding errors**: raising plain `RuntimeError` was rejected because callers cannot tell an embedding failure from other runtime errors. `EmbeddingError(VertebraError, RuntimeError)` keeps existing `except RuntimeError` handlers working.
- **Sidecar model**: pickling the backend callable was rejected because closures and ONNX sessions do not pickle. Running the model in-process on free-threaded builds was rejected because importing `onnxruntime` re-enables the GIL, which the quarantine exists to prevent. An importable factory path is the standard spawn-safe handoff, and the same factory builds the in-process backend, so both paths run the same model.
- **Embedding order**: reordering rows client-side was rejected because the result rows carry no index unless the SQL passes one through; passing `idx` through the model function and ordering the outer query makes positional matching correct with no extra client logic.
- **Mount timeout**: removing the timeout entirely was rejected because `_AsgiMountRunner.__call__` is a buffered path (it returns complete responses, called by `dispatch_batch(async_mode=False)`; the production worker streams through `spawn_batch`), so a stuck request would block the batch forever. A finite 30 s default with a `VERTEBRA_ASGI_BATCH_TIMEOUT` override (`0`/`none` disables) follows the existing `VERTEBRA_INTERFACE` env pattern. Deleting `__call__` was rejected because `dispatch_batch(async_mode=False)` is still a supported entry point.
- **Registry**: the audit described the `spanner+postgresql` branch as emitting an invalid dialect. At `142cc49` `SpannerConfig.from_dsn` rejects the scheme before the override is used, so the branch is dead code. The real defect is that `parse_url` rejects `spangres://`. The plan removes the dead branch and routes `spangres`.

## 3. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task(s) | Verification Strategy | Confirmed Public Seam & Canonical Test Surface |
| --- | --- | --- | --- |
| R1 | `t01` | `regression_tdd` | `src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_sync_tenant_predicate_raises_when_guc_set_fails`, `::test_async_tenant_predicate_raises_when_guc_set_fails` |
| R2 | `t01` | `regression_tdd` | `src/py/tests/unit/db/test_spanner.py::test_registry_parses_spangres_scheme`, `::test_registry_rejects_spanner_postgresql_scheme` |
| R3 | `t01` | `regression_tdd` | `src/py/tests/unit/db/test_spanner_queue_and_cdc.py::test_spanner_change_stream_poll_binds_params_for_dialect` |
| R4 | `t02` | `regression_tdd` | `src/py/tests/unit/extensions/test_vite_and_docs_portal.py::test_webmcp_manifest_lists_only_tagged_tools`, `::test_configurable_docs_portal_and_route_annotated_webmcp` |
| R5 | `t02` | `regression_tdd` | `src/py/tests/unit/agent/test_mcp_conformance.py::test_mcp_tool_call_requires_approved_confirmation_token`, `::test_mcp_confirmation_rejects_approval_by_another_owner`, `::test_confirmation_gate_rejects_unknown_tokens`, `::test_agent_plugin_mcp_confirmation_uses_runtime_gate` |
| R6 | `t03` | `regression_tdd` | `src/py/tests/unit/agent/test_mcp_conformance.py::test_mcp_http_handler_rejects_mcp_name_without_body_name` (nested-params, missing-name, batch-body), `src/py/tests/unit/agent/test_core_and_observability.py::test_jsonrpc_router_passes_dict_params_without_unwrapping`, `::test_jsonrpc_router_does_not_retry_handler_type_errors` |
| R7 | `t04` | `regression_tdd` | `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py::test_is_mutating_sql_classifies_statements`, `::test_is_mutating_sql_rejects_non_trailing_semicolons`, `::test_bigquery_object_table_inspect_rejects_invalid_table_name`, `::test_bigquery_object_table_inspect_accepts_project_qualified_name`, `::test_spanner_query_rejects_multiple_statements`, `::test_spanner_query_requires_confirmation_for_privilege_and_procedure_calls` |
| R8 | `t05` | `regression_tdd` | `src/py/tests/unit/db/test_redis_valkey_driver.py::test_store_delete_all_removes_only_its_prefix`, `::test_store_sends_sub_second_ttl_as_milliseconds`, `::test_store_expires_in_returns_none_without_expiry`, `::test_store_rejects_non_positive_ttl`, `::test_store_rejects_invalid_key_prefix`, `::test_redis_driver_scan_unlink_and_px_in_memory_mode`; `src/py/tests/unit/extensions/test_starlette_plugin.py::test_cache_store_delete_all_removes_only_its_prefix` |
| R9 | `t05` | `regression_tdd` | `src/py/tests/unit/server/test_litestar_direct_fastpath.py::test_litestar_plugin_keeps_memory_channels_and_response_cache_by_default` |
| R10 | `t06` | `regression_tdd` | `src/py/tests/unit/db/test_spanner.py::test_spanner_embedder_propagates_driver_errors`, `::test_spanner_embedder_rejects_row_count_mismatch`, `::test_spanner_embedder_returns_driver_vectors`, `::test_spanner_embedder_builders_reject_invalid_identifiers`, `::test_spanner_embedder_generate_sql_orders_rows_by_input_index`; `src/py/tests/unit/db/test_bigquery.py::test_bigquery_embedder_builders_reject_invalid_identifiers`, `::test_bigquery_embedder_generate_sql_orders_rows_by_input_index`, `::test_bigquery_embedder_rejects_incomplete_results`; `src/py/tests/unit/embeddings/test_google_embedders.py::test_oracle_embedder_raises_embedding_error_on_empty_result` |
| R11 | `t07` | `regression_tdd` | `src/py/tests/unit/embeddings/test_embedding_broker.py::test_sidecar_runs_configured_backend_factory`, `::test_embedding_broker_requires_backend`, `::test_fast_embedder_runs_fastembed_backend_in_process`, `::test_local_embedders_configure_fastembed_backend_factory` |
| R12 | `t08` | `regression_tdd` | `src/py/tests/unit/server/test_runner.py::test_event_loop_pool_stop_drains_tasks_and_async_generators`, `::test_event_loop_pool_raises_when_shard_does_not_start` |
| R13 | `t08` | `regression_tdd` | `src/py/tests/unit/server/test_runner.py::test_asgi_mount_runner_times_out_and_cancels_task`, `::test_asgi_mount_runner_reads_batch_timeout_from_environment`, `::test_asgi_mount_batch_timeout_rejects_invalid_values` |

## 4. Implementation Plan

### Phase 1: Data Layer and MCP Security

- [x] t01 — Tenant predicate raises, Spangres URL routing, and Spangres CDC positional params (tasks/t01-db-tenant-spangres-correctness.md)
- [ ] t02 — WebMCP per-tool opt-in and MCP confirmation enforcement (tasks/t02-mcp-exposure-and-confirmation.md)
- [ ] t03 — `Mcp-Name` binding and nested-params rejection (tasks/t03-mcp-name-binding.md)
- [ ] t04 — DB tool identifier validation and statement classification (tasks/t04-db-tool-sql-safety.md)
- **Phase 1 Recovery Checkpoint**:

  ```bash
  make lint-all
  uv run pytest src/py/tests/unit/db src/py/tests/unit/agent src/py/tests/unit/extensions -q
  ```

### Phase 2: Stores, Embeddings, and Server Shutdown

- [ ] t05 — Store prefix-scoped delete, millisecond TTLs, `expires_in` contract, and Litestar swap defaults (tasks/t05-store-data-safety.md)
- [ ] t06 — Remote embedder error propagation and identifier validation (tasks/t06-remote-embedder-errors.md)
- [ ] t07 — Local embedders run the configured model on free-threaded builds (tasks/t07-local-embedder-sidecar-backend.md)
- [ ] t08 — `EventLoopPool.stop` drain and configurable ASGI mount timeout (tasks/t08-loop-shutdown-and-mount-timeout.md)
- **Phase 2 Recovery Checkpoint**:

  ```bash
  make lint-all
  make py-test
  ```

## 5. Continuity Snapshot

- **Flow ID**: `critical-correctness-fixes` (`state: planned`, `plan_revision: 2`, `plan_commit: 24e2931`)
- **Current task**: none. Start with `t01` on branch `fix/critical-correctness`.
- **Last verified checkpoint**: none.
- **Key decisions**:
  - Driver exceptions propagate unchanged from the tenant GUC `SET`.
  - WebMCP exposure is per-tool opt-in (`@mcp_tool(webmcp=True)`), and `include_tagged_only` is removed.
  - The MCP confirmation token travels in `_meta["vertebra/confirmationToken"]`, the error code is `-32003`, and approval is bound to the ticket's status, owner, tool name, and arguments; `ConfirmationGate.approve`/`reject` mutate state only after the ticket and owner checks pass.
  - MCP routing headers on JSON-RPC batch bodies are rejected.
  - Stores prefix driver keys with `key_prefix` (default `vertebra`) and delete with `SCAN` + `UNLINK`.
  - `EmbeddingError(VertebraError, RuntimeError)` and `BackendFactory` are the only new exported embedding names; generate SQL orders output by input offset.
  - The buffered ASGI mount timeout defaults to 30 s and is set with `VERTEBRA_ASGI_BATCH_TIMEOUT` (`0`/`none` disables).
- **Newest discoveries**:
  - `ConfirmationGate.approve` recorded tokens before its ticket/owner checks, so a failed approval still approved the token.
  - `_AsgiMountRunner.__call__` is reached only from `dispatch_batch(async_mode=False)`; the production worker uses `spawn_batch`, so the old 10 s cap never touched streaming.
  - The `spanner+postgresql` registry branch is dead at `142cc49`: `SpannerConfig.from_dsn` rejects the scheme first. `parse_url("spangres://...")` raises `ValueError`.
  - The existing Spanner embedder test and broker quarantine test pass only because of hash-vector fallbacks.
  - `fastembed` is not installed in the dev environment, so sidecar tests use an importable test factory (`src/py` is on the venv path through the maturin `.pth`).
