---
type: Spec
flow_id: integration-remediation
title: Vertebra Integration and Quality Remediation
description: Master PRD that makes Vertebra's driver, SQLSpec, SQLAlchemy/Advanced Alchemy, Litestar, Django, queue, and MCP integrations correct and feature-complete behind a small public API.
state: planned
parent_prd: null
research:
  - research/audit-findings_20261103.md
  - research/google-cloud-rust_20261103.md
plan_revision: 4
plan_commit: "f741701"
state_revision: 1
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T03:30:00Z
updated_at: "2026-11-05T01:03:53Z"
tags: [prd, remediation, integrations, drivers, sqlspec, queues, mcp]
---

# Master PRD: Vertebra Integration and Quality Remediation

Baseline: `main @ f741701` (Chapters 1 `critical-correctness-fixes`, 2 `gcp-wire-protocol-correctness`, and 9 `gcp-driver-resilience` completed and archived onto `main @ 142cc49`, followed by the codebase-wide `Sync*` / `Async*` class split). Verified audit findings live in [research/audit-findings_20261103.md](research/audit-findings_20261103.md).

## 1. North Star Goal & Problem Framing

### Objective
Vertebra's value is its Rust runtime and drivers. Users primarily reach them through SQLAlchemy / Advanced Alchemy, SQLSpec, Litestar, and Django. This initiative makes those integrations correct and feature-complete, keeps the public API small, and removes code that duplicates framework capabilities or relies on stand-in fakes.

### Remaining gaps on `refactor/sync-async-names`
- **Drivers not yet cut over to native Rust I/O and per-driver wheels** (tracked in sibling PRD [native-data-platform](../native-data-platform/spec.md)):
  - Oracle and MSSQL Python drivers execute against `sqlite3.connect(":memory:")`.
  - PostgreSQL/MySQL/Redis still retain legacy client or `memory://` paths, and native cutovers for relational and embedded engines remain unfinished. BigQuery and Spanner use standalone optional wheels; DataFusion execution fails closed when its dependency is unavailable.
- **Pipelining vs. CTE compilation.**
  - Native protocol pipelining (`SyncPipelineCapable.pipeline()` / `AsyncPipelineCapable.pipeline()`) replaces `db/pipeline.py::{SyncQueryPipeline, AsyncQueryPipeline}`, which compile read steps into a CTE and suppress compilation exceptions in `extensions/sqlspec/adapters.py`.
- **Engine code in generic modules.**
  - `db/cdc.py` mixes PostgreSQL logical replication settings, `pg_logical_emit_message`, unbounded in-memory spill storage, swallowed `SyncObjectStore.get` errors, and `SyncSpannerChangeStreamSource` / `AsyncSpannerChangeStreamSource` polling (`inspect.isawaitable` duck-typing in `AsyncSpannerChangeStreamSource.poll_partition`).
  - `db/channels.py::DatabaseChannelBackend` hardwires PostgreSQL `pg_notify` and Oracle `DBMS_AQ` into the generic channel bus.
  - `db/mcp_tools.py` is GCP-only and imports `vertebra.agent`.
  - `embeddings/{_bigquery,_spanner,_google}.py` embed engine SQL directly inside `vertebra.embeddings`.
- **SQLSpec adapters.**
  - Vertebra stores do not subclass upstream SQLSpec store bases, and schemas drift.
  - `BaseSyncDriver` / `BaseAsyncDriver` override `execute`/`select`, bypassing SQLSpec's dispatch contract and dropping `**kwargs`.
  - Six engines return `None` from `data_dictionary` (only `BigQuerySyncDataDictionary` / `BigQueryAsyncDataDictionary` and `SpannerSyncDataDictionary` / `SpannerAsyncDataDictionary` exist today).
  - Upstream resolves stores by `sqlspec.adapters.*` module prefix ([sqlspec#876](https://github.com/litestar-org/sqlspec/issues/876)).
- **Queues.**
  - PostgreSQL, Oracle, and Spanner first-party queue backends run in memory, and `litestar-queues` has the same module-prefix lookup constraint ([litestar-queues#186](https://github.com/cofin/litestar-queues/issues/186)).
- **MCP / WebMCP protocol core.**
  - Confirmation token enforcement, explicit `webmcp=False` opt-in, and batch header checks landed in Chapter 1, while Flask/Django still carry duplicate `2024-11-05` JSON-RPC dispatchers, schema generation still uses string-annotation `eval`, Litestar does not yet delegate to `litestar-mcp`, and the TypeScript bridge still calls `navigator.modelContext.provideContext`.
- **Structural duplication & hot-path copies.**
  - Per-framework fastpath duplication (~3,000 lines), duplicate cron parsers, `VertexEmbedder` transport overhead, Rust protobuf varint copies, and `_py_*` fallbacks behind mandatory Rust `vertebra.utils.*` helpers.

### What is new and why it succeeds
- Each driver package (`vertebra/db/adapters/<engine>/`) talks to its real database through its per-engine Rust extension wheel `vertebra._<engine>` ([native-data-platform](../native-data-platform/spec.md)) and exposes explicit `Sync*` / `Async*` engine-local primitives (`SyncPipelineCapable` / `AsyncPipelineCapable`, `SyncNotificationSource` / `AsyncNotificationSource`, `SyncChangeSource` / `AsyncChangeSource`, `QueueStore`, `EmbeddingPushdown`).
- Framework extensions (`vertebra.extensions.*`, `vertebra.framework.*`, `vertebra.agent.*`) consume those protocols and implement each framework's native contract without upward imports from `vertebra.db`.

### Success metrics
- Zero production drivers open SQLite for non-SQLite engines or serve `memory://` fake rows; every engine passes `test_driver_conformance.py` via `pytest-databases`.
- `driver.pipeline()` batches N statements in one round trip on PostgreSQL and runs sequentially behind the same API on Oracle until upstream 26ai pipelining ships.
- Spanner and BigQuery Storage run in standalone `vertebra-spanner` (`vertebra._spanner`) and `vertebra-bigquery` (`vertebra._bigquery`) wheels on `google-cloud-spanner` / `google-cloud-bigquery` with `Sync*`/`Async*` client pairs and typed `GoogleCloudError` + `DatabaseError` exceptions (`DatabaseError` multi-base inheritance and wheel extraction completed in `db-layout-and-contract`).
- Vertebra SQLSpec adapters pass upstream ADK, events, and Litestar store contracts.
- First-party queues, `litestar-queues`, and Django tasks deliver across processes on PostgreSQL and Oracle with DI-injected dependencies.
- AST import and engine-purity gates verify `vertebra.db` imports nothing from `vertebra.{agent,framework,extensions,server,embeddings}` and generic `vertebra/db/*` modules contain zero engine leaks.
- MCP `2026-07-28` conformance passes across Starlette, FastAPI, Flask, Django, Litestar, and native `App`.

### Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Upstream sqlspec#876 / litestar-queues#186 land late | Temporary resolver shim in `extensions/sqlspec/_compat.py`, guarded by a pinning test that fails once upstream ships so the shim is removed |
| Clean-break renames affect callers | Pre-1.0 clean-break policy (zero compatibility shims); document renames in docs and changelog |
| Emulator feature gaps (Spanner multiplexed sessions, BigQuery Storage Write) | Emulator-only gaps marked `xfail(strict=True)` with reason; behavior verified via official crates' `from_stub` seams |
| Real driver cutovers surface latent assumptions | Switch over one engine flow at a time against `test_driver_conformance.py` and `pytest-databases` |

## 2. Locked Architectural Seams & Alternatives Considered

```mermaid
flowchart LR
    Rust["vertebra-_engine wheels"] --> Adapters["vertebra.db.adapters.<engine>"]
    Adapters --> Contracts["vertebra.db.driver Sync/Async protocols"]
    Contracts --> SQLSpec["extensions/sqlspec/adapters"]
    Contracts --> Alchemy["extensions/sqlalchemy and alchemy"]
    Contracts --> Fw["litestar, django, framework, agent"]
```

1. **Layering**.
   - `vertebra.db` holds drivers, configs, and basic engine-local primitives.
   - Framework and agent integrations live in `vertebra.extensions.*`, `vertebra.framework.*`, and `vertebra.agent.tools` (`bigquery.py`, `spanner.py`).
   - `vertebra.db` never imports upward into `agent`, `framework`, `extensions`, `server`, or `embeddings`.
2. **Engine packages & wheels**.
   - Engine code lives in `vertebra/db/adapters/<engine>/{config,core,driver}.py`, plus optional `pipeline.py`, `notify.py`, `change_stream.py`, `queue.py`, and `embeddings.py`, backed by per-engine PyO3 extension crates `src/rs/vertebra-<engine>` (`vertebra._<engine>`) and `[project.optional-dependencies].<engine>`.
   - Engine-agnostic code stays directly under `vertebra/db/` (`driver/`, `registry.py`, `arrow.py`, `cdc.py`, `rls.py`, `dialer/`).
   - Owned by [native-data-platform](../native-data-platform/spec.md) Chapter 1 (`db-layout-and-contract`).
3. **Driver I/O & `Sync*` / `Async*` split** *(user decision: required, feature complete)*.
   - Vertebra's own Rust drivers back every engine (`native-data-platform`) with explicit `Sync*` and `Async*` client/driver/capability classes, any Advanced Alchemy / SQLAlchemy or SQLSpec config shares one broker-owned pool over SHM (`broker-pool-sharing`), and `SyncObjectStore` / `AsyncObjectStore` are complete across frameworks (`storage-completeness`).
4. **Native pipelining.**
   - `driver.pipeline()` (`SyncPipelineCapable` / `AsyncPipelineCapable`) is implemented in `vertebra-postgres` (single-`Sync` extended-protocol batch) and sequentially in `vertebra-oracle` until upstream 26ai pipelining lands.
   - `db/pipeline.py::{SyncQueryPipeline, AsyncQueryPipeline}`, `RelOverlay`, and `CTEStep` are deleted in `db-engine-primitives`, and `execute_stack` dispatches via `isinstance(self.native_driver, SyncPipelineCapable)` / `isinstance(self.native_driver, AsyncPipelineCapable)`.
5. **Registry polymorphism.**
   - `BaseDatabaseConfig` declares `url_schemes`, `driver_module`, `sync_driver_name`, `async_driver_name`, `from_url`, `pool_key`, and `build_sync_driver`/`build_async_driver`; `DatabaseRegistry` is table-driven (including `@overload`ed `get_dialer(config, *, sync=False)`); `provide_driver(name_or_config, *, sync=False, registry=None)` provides generic driver resolution ([database foundation](../../knowledge/domains/data/database-drivers-and-dialers.md)).
6. **SQLSpec adapter parity**.
   - Layout: `extensions/sqlspec/adapters/<engine>/{config,driver,core,data_dictionary,adk/,events/,litestar/}.py`, subclassing upstream bases and schemas with `adapter_name: ClassVar[str]` and preserving upstream SQLSpec `<Engine>SyncConfig` / `<Engine>AsyncConfig` and `<Engine>SyncDriver` / `<Engine>AsyncDriver` infix naming inside `vertebra.extensions.sqlspec`.
7. **Queues**.
   - Native `db/adapters/{postgres,oracle}/queue.py` (`QueueStore`) back first-party `QueueService`, `litestar-queues` `BaseQueueBackend` classes, and Django `django.tasks`, resolving dependencies through `framework/di.py::Container`.
8. **MCP**.
   - One frozen msgspec `ToolSpec` and agent-owned `ToolRegistry` consume existing `vertebra.typing.ToolProvider`, established in `db-engine-primitives:t05`. Native `BigQueryToolProvider`/`SpannerToolProvider` remain on their owning adapters; `vertebra.agent.tools.build_tools` supplies generic typed invocation and per-call confirmation. Transport-agnostic `MCPCore.handle()`, thin ASGI/WSGI/native adapters, WebMCP projection, and litestar-mcp delegation remain owned by `mcp-webmcp-core`, consuming this contract without a second provider definition or vendor dispatch in generic agent code.
9. **Rust GCP clients** *(completed in Chapters 2 and 9, extracted to per-driver wheels in `db-layout-and-contract`)*.
   - Spanner and BigQuery Storage Read/Write run on `google-cloud-spanner =0.34.8-preview` (`vertebra-spanner`) and `google-cloud-bigquery =0.19.0` (`vertebra-bigquery`) (`rustls` + `ring`) bridged to `SyncGcpTokenCache` / `AsyncGcpTokenCache`, with panic/cancel-safe async completion over `AsyncCompletionQueue` and typed `GoogleCloudError` + `DatabaseError` hierarchy.

### Alternatives rejected
- **Per-feature subpackages (`db/cdc/{postgres,spanner}.py`).** Splits one engine across multiple packages and loses locality with its driver.
- **Monolithic `vertebra._vertebra` linking every database SDK.** Pulls gRPC, wire, and C++ dependencies into `vertebra-core` for users who only install one engine or HTTP-only Vertebra.
- **Dual-mode classes mixing sync and `*_async` methods.** Replaced codebase-wide on `refactor/sync-async-names` with explicit `Sync*` and `Async*` classes exposing plain method names.
- **Deleting Vertebra SQLSpec stores in favor of upstream-only.** Feature-complete Vertebra adapters are required.
- **Native stores without upstream bases.** Causes schema drift and breaks `SQLSpecSessionService` and extension migrations.
- **Keeping `SyncQueryPipeline` / `AsyncQueryPipeline` alongside `driver.pipeline()`.** A CTE wrapper is not wire pipelining, cannot mix arbitrary DML safely, and duplicates SQLSpec `StatementStack`.
- **One MCP core including Litestar.** Duplicates `litestar-mcp`, which already conforms to `2026-07-28`.

## 3. Master Roadmap (Ordered Child Flows)

| Ch | Flow ID | Title | Key Deliverables | Public Seams | Depends on | State |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `critical-correctness-fixes` | Silent failure and security fixes | Confirmation-gate binding, opt-in WebMCP, prefix-scoped `SCAN`+`UNLINK`, fail-loud embeddings + `ORDER BY idx`, ASGI batch timeout | existing APIs | — | completed |
| 2 | `gcp-wire-protocol-correctness` | Official Google Cloud Rust clients for Spanner and BigQuery Storage | `google-cloud-spanner` / `google-cloud-bigquery` migration, commit/DDL/append verification, hardened `ArrowAppendPlan`, `grpc.rs` deleted | `SyncSpannerClient`/`AsyncSpannerClient`, `SyncBigQueryClient`/`AsyncBigQueryClient` | — | completed |
| 3 | `db-engine-primitives` | Engine primitives relocation and SyncQueryPipeline / AsyncQueryPipeline removal | CDC/notify/change-stream/embedding pushdown moved to `db/adapters/<engine>`, `ChannelBus` decoupled (`db/channels.py` deleted), `SyncQueryPipeline`/`AsyncQueryPipeline` deleted (`SyncPipelineCapable`/`AsyncPipelineCapable` dispatch), adapter-owned GCP ToolProviders, neutral typing contract, and generic agent.tools consumer, dialect-agnostic `db/rls.py` | `vertebra.db.cdc`, `vertebra.db.adapters.*.{notify,change_stream,embeddings}`, `PushdownEmbedder`, `vertebra.agent.tools`, `vertebra.typing.ToolProvider`, adapter provider classes | 1, 2, 9; native-data-platform Ch1 | planned |
| 4 | `sqlspec-adapter-parity` | Feature-complete SQLSpec adapters | Per-engine adapter packages, dispatch-contract drivers, upstream-base stores, `Sync`/`Async` data dictionaries, `_compat.py` resolver shim, shared-pool refcount, `VTBARROW` broker results | `<Engine>{Sync,Async}Config`, `adapter_name` | 3; native-data-platform Ch1–Ch6 | draft |
| 5 | `queues-tasks-and-channels` | Working queues, tasks, channels, stores | Native `PostgresQueueStore` / `OracleQueueStore`, settings-driven `QueueService`, `litestar-queues` & Django tasks backends with DI, single cron parser, distributed Litestar/Starlette channels, `ValkeyStore` | `QueueService`, `QueueBackend`, `QueueBackendSettings` | 3, 4; native-data-platform Ch2, Ch4, Ch6 | draft |
| 6 | `sqlalchemy-and-django-integration` | SQLAlchemy / AA / Django DB integration | `Sync<Engine>Dialect` / `Async<Engine>Dialect` + official GCP/DuckDB dialect bases over shared `dbapi.py` (`SyncDBAPI*`/`AsyncDBAPI*`), AA dogpile cache backend, real Django backends including Spanner via `django_spanner` | dialect entry points, `create_cache_config`, Django `ENGINE` paths | 3; native-data-platform Ch1, Ch3, Ch7 | draft |
| 7 | `mcp-webmcp-core` | MCP 2026-07-28 core and WebMCP bridge | `ToolSpec`/`ToolRegistry` consuming neutral `ToolProvider`/`MCPCore`, ASGI/WSGI/native adapters, `litestar-mcp` delegation, TS bridge on `document.modelContext`, conformance | `ToolSpec`, `MCPCore`, `registerWebMCPTools` | 1, 3 | draft |
| 8 | `runtime-structure-and-throughput` | Fastpath, Rust modules, helpers | Shared `_fastpath_core.py` + `FastPathProvider`, table-driven loops, Rust `proto.rs` + `vertebra-{spanner,bigquery}` submodule splits + shared `py_async`, zero-copy Arrow append, `VertexEmbedder` HTTP/2 transport, `utils` fallback removal | `FastPathProvider`, `vertebra.utils.*`, `_vertebra.pyi` | 2, 3, 9; native-data-platform Ch1 | draft |
| 9 | `gcp-driver-resilience` | GCP driver resilience and typed errors | Panic-safe/cancellable `spawn_rust_db_task`, typed `GoogleCloudError` hierarchy across sync/async bindings, async `gcloud` + `ProxyPolicy` + explicit scopes, BigQuery REST warning/URL fixes, `char`-aware Spangres lexer | `_vertebra.pyi` exceptions, `TaskPanicError`, `{Sync,Async}GcpTokenCache.from_options` | 2 | completed |

## 4. Global Constraints (Applicable to All Child Flows)

- **Branching.** Never commit or push directly to `main`; each flow lands on a dedicated feature/fix branch with signed conventional commits.
- **Clean break & `Sync*` / `Async*` naming.**
  - Leading `Sync*` and `Async*` prefixes on all dual-mode Rust and Python classes (`SyncCompletionQueue` / `AsyncCompletionQueue`, `Sync<Engine>Client` / `Async<Engine>Client`, `SyncObjectStore` / `AsyncObjectStore`, `Sync<Engine>Dialect` / `Async<Engine>Dialect`), with plain un-suffixed method names and zero mixed `*_async` methods (preserving upstream `sqlspec` `<Engine>SyncConfig` / `<Engine>AsyncConfig` infix naming in `vertebra.extensions.sqlspec` and required dual-mode external protocol adapters).
  - Zero backward-compatibility aliases, re-export shims, or one-time upgrade flags.
  - Zero `.get()`/`try-except` fallbacks added for synthetic test seeds; update tests to match the production contract.
  - Zero emulator hooks or in-memory/SQLite fallbacks in production driver classes.
- **Python style.**
  - `msgspec.Struct(..., frozen=True, gc=False)` value objects (`array_like=True` on hot wire frames) with inline field docstrings.
  - PEP 585/604/695 types, Google docstrings, zero inline `#` comments, top-level imports (`TYPE_CHECKING` / cached `module_loader` helpers for optional deps), class layout (`attributes -> __init__ -> properties -> public -> private`), centralized `vertebra.utils.serializers`, and `@pytest.mark.parametrize` / parameterized fixtures instead of test loops.
- **Public helpers.** `vertebra.utils.*` functions remain public user helpers regardless of in-repo caller count.
- **Small API surface.** Add public symbols only where a framework or capability contract requires them; Advanced Alchemy / SQLAlchemy remains the primary user path.
- **Tests.** Place tests in existing canonical module test files (never standalone `test_*remediation*.py` or `test_*audit*.py`).
- **Docs and knowledge.** Each child flow updates affected `docs/` pages and `.agents/bundles/knowledge/` chapters so `make docs` and `make review-check` stay green at every checkpoint.

## 5. Backlog (Deferred Items)

- Spanner and Redis/Valkey native `QueueStore` backends built on the PostgreSQL/Oracle pattern.
- AWS RDS/Aurora DSQL IAM dialer ([vertebra#67](https://github.com/cofin/vertebra/issues/67)) and Azure Entra ID dialer ([vertebra#68](https://github.com/cofin/vertebra/issues/68)).
- Removing `extensions/sqlspec/_compat.py` once [sqlspec#876](https://github.com/litestar-org/sqlspec/issues/876) and [litestar-queues#186](https://github.com/cofin/litestar-queues/issues/186) ship.
- Interactive server-side multi-statement Spanner `ReadWrite` session RPCs outside `run_in_transaction` (`driver.transaction()` buffers DML and mutations for atomic flush in `db-layout-and-contract`).

## Implementation Plan

- [x] [364c7cb] critical-correctness-fixes — Silent failure and security fixes (archived in `log.md`)
- [x] [984ce73] gcp-wire-protocol-correctness — Official Google Cloud Rust clients for Spanner and BigQuery Storage (archived in `log.md`)
- [x] [abd33ab] gcp-driver-resilience — GCP driver resilience and typed errors (archived in `log.md`)
- [ ] db-engine-primitives — Engine primitives relocation and SyncQueryPipeline / AsyncQueryPipeline removal (../db-engine-primitives/spec.md)
- [ ] sqlspec-adapter-parity — Feature-complete SQLSpec adapters (../sqlspec-adapter-parity/spec.md)
- [ ] queues-tasks-and-channels — Working queues, tasks, channels, stores (../queues-tasks-and-channels/spec.md)
- [ ] sqlalchemy-and-django-integration — SQLAlchemy / AA / Django DB integration (../sqlalchemy-and-django-integration/spec.md)
- [ ] mcp-webmcp-core — MCP 2026-07-28 core and WebMCP bridge (../mcp-webmcp-core/spec.md)
- [ ] runtime-structure-and-throughput — Fastpath, Rust modules, helpers (../runtime-structure-and-throughput/spec.md)

## Continuity Snapshot

- **Flow ID:** `integration-remediation` (`state: planned`, `plan_revision: 4`, `plan_commit: "f741701"`)
- **Active Focus:** Chapter 3 (`db-engine-primitives`, `t01`–`t07`) remains planned. Its database layout, capability, registry, and wheel prerequisites are complete; engine primitive relocation and pipeline removal remain unfinished.
- **Completed Checkpoints:** `critical-correctness-fixes` (`364c7cb`), `gcp-wire-protocol-correctness` (`984ce73`), and `gcp-driver-resilience` (`abd33ab`).
- **Key decisions:**
  - Engine adapters and local primitives live under `vertebra/db/adapters/<engine>/` backed by per-engine PyO3 wheels `src/rs/vertebra-<engine>` (`vertebra._<engine>`); framework and agent integrations live under `vertebra.extensions.*`, `vertebra.framework.*`, and `vertebra.agent.tools`.
  - `db-engine-primitives` dispatches `execute_stack` on `SyncPipelineCapable` / `AsyncPipelineCapable`, using the existing protocols in the [database foundation](../../knowledge/domains/data/database-drivers-and-dialers.md).
