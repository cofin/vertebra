---
type: Spec
flow_id: integration-remediation
title: Vertebra Integration and Quality Remediation
description: Master PRD that makes Vertebra's driver, SQLSpec, SQLAlchemy/Advanced Alchemy, Litestar, Django, queue, and MCP integrations correct and feature-complete behind a small public API.
status: stable
state: planned
has_chapters: false
parent_prd: null
research:
  - research/audit-findings_20261103.md
  - research/google-cloud-rust_20261103.md
plan_revision: 8
plan_commit: fd571e3a
state_revision: 3
current_task: null
last_verified_checkpoint: fd571e3a
created_at: "2026-11-03T03:30:00Z"
updated_at: "2026-11-08T20:15:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T04:15:00Z"
verified:
  - by: agent:flow-revise
    at: "2026-11-08T20:15:00Z"
    commit: fd571e3a
  - by: agent:flow-setup
    at: "2026-11-07T04:15:00Z"
    commit: cdb978d3
sources:
  - id: db-registry
    resource: src/py/vertebra/db/registry.py::DatabaseRegistry
    title: Table-driven DatabaseRegistry and provide_driver polymorphism
  - id: fastpath-core
    resource: src/py/vertebra/server/_fastpath_core.py
    title: Shared framework fastpath provider core delivered in runtime-structure-and-throughput
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: SQLSpec adapter integration surface targeted by sqlspec-adapter-parity
tags:
  - prd
  - remediation
  - integrations
  - drivers
  - sqlspec
  - queues
  - mcp
---

# Master PRD: Vertebra Integration and Quality Remediation

<!-- truth: start -->
- **Architectural Layering**: `vertebra.db` owns drivers, configs, and engine-local primitives (`vertebra/db/adapters/<engine>/`) without upward imports into `agent`, `framework`, `extensions`, `server`, or `embeddings` [^db-registry].
- **Completed Foundation on `main @ 077a3fe9`**: Chapters 1 (`364c7cbe`), 2 (`984ce732`), 3 (`80ff8a65`), 7 (`077a3fe9`), 8 (`d2fa5072`), and 9 (`abd33ab6`) are merged on `main`, establishing typed GCP Rust wheels, adapter-local primitives, shared `_fastpath_core.py`, and MCP `2026-07-28` / WebMCP conformance [^fastpath-core].
- **Remaining Open Child Flows**: `sqlspec-adapter-parity` (Ch4), `queues-tasks-and-channels` (Ch5), and `sqlalchemy-and-django-integration` (Ch6) remain in `draft` sequenced behind sibling [native-data-platform](../native-data-platform/spec.md) driver and broker cutovers [^sqlspec-adapters].
<!-- truth: end -->

Baseline: `main @ 077a3fe9` / `HEAD @ fd571e3a` (Chapters 1 `critical-correctness-fixes` [`364c7cbe`], 2 `gcp-wire-protocol-correctness` [`984ce732`], 9 `gcp-driver-resilience` [`abd33ab6`], the `Sync*` / `Async*` class split [`f7417015`], `db-layout-and-contract` [`21c99f95`], Chapter 3 `db-engine-primitives` [`80ff8a65`], post-merge review remediation [`2b737240`], Chapter 8 `runtime-structure-and-throughput` [`d2fa5072`], and Chapter 7 `mcp-webmcp-core` [`077a3fe9`] completed and merged onto `main`, and sibling `storage-completeness` [`a2af5066`] completed and archived on the feature branch). Verified audit findings live in [research/audit-findings_20261103.md](research/audit-findings_20261103.md).

## 1. North Star Goal & Problem Framing

### Objective
Vertebra's value is its Rust runtime and drivers. Users primarily reach them through SQLAlchemy / Advanced Alchemy, SQLSpec, Litestar, and Django. This initiative makes those integrations correct and feature-complete, keeps the public API small, and removes code that duplicates framework capabilities or relies on stand-in fakes.

### Remaining gaps on `main @ 077a3fe9`
- **Drivers not yet cut over to native Rust I/O and per-driver wheels** (tracked as 1 PR per engine in sibling PRD [native-data-platform](../native-data-platform/spec.md)):
  - Oracle and MSSQL Python drivers execute against `sqlite3.connect(":memory:")`.
  - PostgreSQL, MySQL, SQLite, DuckDB, and Redis/Valkey still retain Python client, `sqlite3`, or `memory://` execution paths pending their per-engine Rust wheel cutovers (`native-postgres-driver`, `native-oracle-driver`, `native-mysql-driver`, `native-mssql-driver`, `native-sqlite-driver`, `native-duckdb-driver`, `native-redis-driver`). BigQuery and Spanner use standalone optional wheels (`vertebra-bigquery`, `vertebra-spanner`); `DataFusionEngine` fails closed when `datafusion` is unavailable.
- **SQLSpec adapters** [^sqlspec-adapters].
  - Vertebra stores do not subclass upstream SQLSpec store bases, and schemas drift.
  - `BaseSyncDriver` / `BaseAsyncDriver` override `execute`/`select`, bypassing SQLSpec's dispatch contract and dropping `**kwargs`.
  - Six engines return `None` from `data_dictionary` (only `BigQuerySyncDataDictionary` / `BigQueryAsyncDataDictionary` and `SpannerSyncDataDictionary` / `SpannerAsyncDataDictionary` exist today).
  - Upstream resolves stores by `sqlspec.adapters.*` module prefix ([sqlspec#876](https://github.com/litestar-org/sqlspec/issues/876)).
- **Queues.**
  - PostgreSQL, Oracle, and Spanner first-party queue backends run in memory, and `litestar-queues` has the same module-prefix lookup constraint ([litestar-queues#186](https://github.com/cofin/litestar-queues/issues/186)).
- **SQLAlchemy / Advanced Alchemy & Django database backends.**
  - Relational and cloud dialects still await the framework-neutral `vertebra.db.dbapi` extraction (`broker-pool-sharing`) and full SQLAlchemy / Advanced Alchemy cache + Django `ENGINE` parity (`sqlalchemy-and-django-integration`).

### What is new and why it succeeds
- Each driver package (`vertebra/db/adapters/<engine>/`) talks to its real database through its per-engine Rust extension wheel `vertebra._<engine>` ([native-data-platform](../native-data-platform/spec.md)) and exposes explicit `Sync*` / `Async*` engine-local primitives (`SyncPipelineCapable` / `AsyncPipelineCapable`, `SyncNotificationSource` / `AsyncNotificationSource`, `SyncChangeSource` / `AsyncChangeSource`, `QueueStore`, `EmbeddingPushdown`).
- Framework extensions (`vertebra.extensions.*`, `vertebra.framework.*`, `vertebra.agent.*`) consume those protocols and implement each framework's native contract without upward imports from `vertebra.db` [^fastpath-core].

### Success metrics
- Zero production drivers open SQLite for non-SQLite engines or serve `memory://` fake rows; every engine passes `test_driver_conformance.py` via `pytest-databases`.
- `driver.pipeline()` batches N statements in one round trip on PostgreSQL and runs sequentially behind the same API on Oracle until upstream 26ai pipelining ships.
- Spanner and BigQuery Storage run in standalone `vertebra-spanner` (`vertebra._spanner`) and `vertebra-bigquery` (`vertebra._bigquery`) wheels on `google-cloud-spanner` / `google-cloud-bigquery` with `Sync*`/`Async*` client pairs and typed `GoogleCloudError` + `DatabaseError` exceptions (`DatabaseError` multi-base inheritance and wheel extraction completed in `db-layout-and-contract`).
- Vertebra SQLSpec adapters pass upstream ADK, events, and Litestar store contracts.
- First-party queues, `litestar-queues`, and Django tasks deliver across processes on PostgreSQL and Oracle with DI-injected dependencies.
- AST import and engine-purity gates verify `vertebra.db` imports nothing from `vertebra.{agent,framework,extensions,server,embeddings}` and generic `vertebra/db/*` modules contain zero engine leaks.
- MCP `2026-07-28` conformance passes across Starlette, FastAPI, Flask, Django, Litestar, and native `App` (delivered in `mcp-webmcp-core` [`077a3fe9`]).

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
   - Native `BigQueryToolProvider` and `SpannerToolProvider` implementations live in `db/adapters/<engine>/tools.py` and implement neutral `vertebra.typing.ToolProvider`. Framework integrations remain in `vertebra.extensions.*` / `vertebra.framework.*`; `vertebra.agent.tools.build_tools` is their generic typed/confirmation consumer, with no vendor modules or engine dispatch.
   - `vertebra.db` never imports upward into `agent`, `framework`, `extensions`, `server`, or `embeddings`.
2. **Engine packages & wheels**.
   - Engine code lives in `vertebra/db/adapters/<engine>/{config,core,driver}.py`, plus optional `pipeline.py`, `notify.py`, `change_stream.py`, `queue.py`, and `embeddings.py`, backed by per-engine PyO3 extension crates `src/rs/vertebra-<engine>` (`vertebra._<engine>`) and `[project.optional-dependencies].<engine>`.
   - Engine-agnostic code stays directly under `vertebra/db/` (`driver/`, `registry.py`, `arrow.py`, `cdc.py`, `rls.py`, `dialer/`, and framework-neutral `dbapi/` extracted by sibling `native-data-platform/broker-pool-sharing`).
   - The adapter foundation belongs to [native-data-platform](../native-data-platform/spec.md) Chapter 1 (`db-layout-and-contract`); the neutral DB-API extraction belongs to Chapter 3 (`broker-pool-sharing`).
3. **Driver IO & `Sync*` / `Async*` split** *(user decision: required, feature complete)*.
   - Vertebra's own Rust drivers back every engine (`native-data-platform`) with explicit `Sync*` and `Async*` client/driver/capability classes, any Advanced Alchemy / SQLAlchemy or SQLSpec config shares one broker-owned pool over SHM (`broker-pool-sharing`), and `SyncObjectStore` / `AsyncObjectStore` are complete across frameworks (`storage-completeness`).
4. **Native pipelining.**
   - `driver.pipeline()` (`SyncPipelineCapable` / `AsyncPipelineCapable`) is implemented in `vertebra-postgres` (single-`Sync` extended-protocol batch) and sequentially in `vertebra-oracle` until upstream 26ai pipelining lands.
   - `db/pipeline.py::{SyncQueryPipeline, AsyncQueryPipeline}`, `RelOverlay`, and `CTEStep` are deleted in `db-engine-primitives`, and `execute_stack` dispatches via `isinstance(self.native_driver, SyncPipelineCapable)` / `isinstance(self.native_driver, AsyncPipelineCapable)`.
5. **Registry polymorphism** [^db-registry].
   - `BaseDatabaseConfig` declares `url_schemes`, `driver_module`, `sync_driver_name`, `async_driver_name`, `from_url`, `pool_key`, and `build_sync_driver`/`build_async_driver`; `DatabaseRegistry` is table-driven (including `@overload`ed `get_dialer(config, *, sync=False)`); `provide_driver(name_or_config, *, sync=False, registry=None)` provides generic driver resolution ([database foundation](../../knowledge/domains/data/database-drivers-and-dialers.md)).
   - Sibling `native-data-platform/broker-pool-sharing` owns the first DB-API extraction directly into `vertebra.db.dbapi` (`SyncDBAPI*` / `AsyncDBAPI*`), consuming generic registry/driver/lease contracts without engine dispatch or fallback databases. SQLAlchemy async wrapping stays in `extensions/sqlalchemy/async_adapter.py`; SQLAlchemy/Django integration consumes and hardens the same neutral package without an intermediate extension-owned DB-API. Preserve established execution/cursor/Arrow behavior and resolve concrete metadata/async-connect research gaps before task refinement; this layout adds no result API or policy registry.
6. **SQLSpec adapter parity** [^sqlspec-adapters].
   - Layout: `extensions/sqlspec/adapters/<engine>/{config,driver,core,data_dictionary,adk/,events/,litestar/}.py`, subclassing upstream bases and schemas with `adapter_name: ClassVar[str]` and preserving upstream SQLSpec `<Engine>SyncConfig` / `<Engine>AsyncConfig` and `<Engine>SyncDriver` / `<Engine>AsyncDriver` infix naming inside `vertebra.extensions.sqlspec`.
7. **Queues**.
   - Native `db/adapters/{postgres,oracle}/queue.py` (`QueueStore`) back first-party `QueueService`, `litestar-queues` `BaseQueueBackend` classes, and Django `django.tasks`, resolving dependencies through `framework/di.py::Container`.
8. **MCP** *(completed in Chapter 7 `mcp-webmcp-core` [`077a3fe9`])*.
   - One frozen msgspec `ToolSpec` and agent-owned `ToolRegistry` consume `vertebra.typing.ToolProvider`. Native `BigQueryToolProvider`/`SpannerToolProvider` live on their owning adapters, `vertebra.agent.tools.build_tools` supplies generic typed invocation and per-call confirmation, and transport-agnostic `MCPCore.handle()`, thin ASGI/WSGI/native adapters, `document.modelContext` WebMCP projection, and `litestar-mcp` delegation are merged on `main` (`077a3fe9`) without vendor dispatch in generic agent code.
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
| 3 | `db-engine-primitives` | Engine primitives relocation and SyncQueryPipeline / AsyncQueryPipeline removal | CDC/notify/change-stream/embedding pushdown moved to `db/adapters/<engine>`, `ChannelBus` decoupled (`db/channels.py` deleted), `SyncQueryPipeline`/`AsyncQueryPipeline` deleted (`SyncPipelineCapable`/`AsyncPipelineCapable` dispatch), adapter-owned GCP ToolProviders, neutral typing contract, and generic agent.tools consumer, dialect-agnostic `db/rls.py` | `vertebra.db.cdc`, `vertebra.db.adapters.*.{notify,change_stream,embeddings}`, `PushdownEmbedder`, `vertebra.agent.tools`, `vertebra.typing.ToolProvider`, adapter provider classes | 1, 2, 9; native-data-platform Ch1 | completed |
| 4 | `sqlspec-adapter-parity` | Feature-complete SQLSpec adapters | Per-engine adapter packages, dispatch-contract drivers, upstream-base stores, `Sync`/`Async` data dictionaries, `_compat.py` resolver shim, shared-pool refcount, `VTBARROW` broker results | `<Engine>{Sync,Async}Config`, `adapter_name` | 3; native-data-platform Ch1–Ch8 | draft |
| 5 | `queues-tasks-and-channels` | Working queues, tasks, channels, stores | Native `PostgresQueueStore` / `OracleQueueStore`, settings-driven `QueueService`, `litestar-queues` & Django tasks backends with DI, single cron parser, distributed Litestar/Starlette channels, `ValkeyStore` | `QueueService`, `QueueBackend`, `QueueBackendSettings` | 3, 4; native-data-platform Ch2, Ch4, Ch9 | draft |
| 6 | `sqlalchemy-and-django-integration` | SQLAlchemy / AA / Django DB integration | `Sync<Engine>Dialect` / `Async<Engine>Dialect` + official GCP/DuckDB dialect bases over framework-neutral `db/dbapi/` (`SyncDBAPI*`/`AsyncDBAPI*`) from `broker-pool-sharing`, SQLAlchemy-only `async_adapter.py`, AA dogpile cache backend, real Django backends including Spanner via `django_spanner` | dialect entry points, `create_cache_config`, Django `ENGINE` paths | 3; native-data-platform Ch1, Ch3, Ch10 | draft |
| 7 | `mcp-webmcp-core` | MCP 2026-07-28 core and WebMCP bridge | `ToolSpec`/`ToolRegistry` consuming neutral `ToolProvider`/`MCPCore`, ASGI/WSGI/native adapters, `litestar-mcp` delegation, TS bridge on `document.modelContext`, conformance | `ToolSpec`, `MCPCore`, `registerWebMCPTools` | 1, 3 | completed |
| 8 | `runtime-structure-and-throughput` | Fastpath, Rust modules, helpers | Shared `_fastpath_core.py` + `FastPathProvider`, table-driven loops, Rust `proto.rs` + `vertebra-{spanner,bigquery}` submodule splits + shared `py_async`, zero-copy Arrow append, `VertexEmbedder` HTTP/2 transport, `utils` fallback removal | `FastPathProvider`, `vertebra.utils.*`, `_vertebra.pyi` | 2, 3, 9; native-data-platform Ch1 | completed |
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

- [x] [364c7cbe] critical-correctness-fixes — Silent failure and security fixes (archived in `log.md`)
- [x] [984ce732] gcp-wire-protocol-correctness — Official Google Cloud Rust clients for Spanner and BigQuery Storage (archived in `log.md`)
- [x] [abd33ab6] gcp-driver-resilience — GCP driver resilience and typed errors (archived in `log.md`)
- [x] [80ff8a65] db-engine-primitives — Engine-owned primitives, neutral ToolProviders, native stacks, and tenant boundaries (contracted in [query-pipeline-cdc-and-tenancy.md](../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md))
- [ ] sqlspec-adapter-parity — Feature-complete SQLSpec adapters ([spec.md](../sqlspec-adapter-parity/spec.md))
- [ ] queues-tasks-and-channels — Working queues, tasks, channels, stores ([spec.md](../queues-tasks-and-channels/spec.md))
- [ ] sqlalchemy-and-django-integration — SQLAlchemy / AA / Django DB integration ([spec.md](../sqlalchemy-and-django-integration/spec.md))
- [x] [077a3fe9] mcp-webmcp-core — MCP 2026-07-28 core and WebMCP bridge (archived in `log.md`)
- [x] [d2fa5072] runtime-structure-and-throughput — Fastpath, Rust modules, helpers (archived in `log.md`)

## Continuity Snapshot

- **Flow ID:** `integration-remediation` (`state: planned`, `plan_revision: 8`, `plan_commit: fd571e3a`, `last_verified_checkpoint: fd571e3a`)
- **Active Focus:** Chapters 1, 2, 3, 7 (`077a3fe9`), 8 (`d2fa5072`), and 9 are merged onto `main @ 077a3fe9` (`HEAD @ fd571e3a`), and sibling `storage-completeness` (`a2af5066`) is archived on the feature branch. Sibling Wave 1 flows `storage-remediation`, `driver-fail-loud`, and `native-postgres-driver` execute next ahead of Chapters 4 (`sqlspec-adapter-parity`), 5 (`queues-tasks-and-channels`), and 6 (`sqlalchemy-and-django-integration`).
- **Completed Checkpoints:** `critical-correctness-fixes` (`364c7cbe`), `gcp-wire-protocol-correctness` (`984ce732`), `gcp-driver-resilience` (`abd33ab6`), `db-engine-primitives` (`80ff8a65`), `runtime-structure-and-throughput` (`d2fa5072`), and `mcp-webmcp-core` (`077a3fe9`).
- **Key decisions:**
  - Engine adapters and local primitives live under `vertebra/db/adapters/<engine>/` backed by per-engine PyO3 wheels `src/rs/vertebra-<engine>` (`vertebra._<engine>`); framework and agent integrations live under `vertebra.extensions.*`, `vertebra.framework.*`, and `vertebra.agent.tools`.
  - `db-engine-primitives` dispatches `execute_stack` on `SyncPipelineCapable` / `AsyncPipelineCapable`, using the protocols in the [database foundation](../../knowledge/domains/data/database-drivers-and-dialers.md).

[^db-registry]: `src/py/vertebra/db/registry.py::DatabaseRegistry` — Table-driven database driver and dialer registry.
[^fastpath-core]: `src/py/vertebra/server/_fastpath_core.py` — Shared `FastPathProvider` core across ASGI/WSGI/native frameworks.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — SQLSpec adapter configuration and driver bindings.
