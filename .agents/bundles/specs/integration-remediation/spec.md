---
type: Spec
flow_id: integration-remediation
title: Vertebra Integration and Quality Remediation
description: Master PRD that makes Vertebra's driver, SQLSpec, SQLAlchemy/Advanced Alchemy, Litestar, Django, queue, server runtime, embedding, and MCP integrations correct and feature-complete behind a small public API.
status: draft
state: planned
has_chapters: false
parent_prd: null
research:
  - research/audit-findings_20261103.md
  - research/google-cloud-rust_20261103.md
plan_revision: 10
plan_commit: null
state_revision: 6
current_task: null
last_verified_checkpoint: c8722c76
created_at: "2026-11-03T03:30:00Z"
updated_at: "2026-11-12T01:30:00Z"
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified: []
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
  - id: sa-dbapi
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection
    title: PEP 249 connection opening a second sqlite3 connection and dispatching on concrete config classes
  - id: queue-redis
    resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend
    title: Redis and Valkey queue backends building redis.asyncio clients
  - id: queue-oracle
    resource: src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend
    title: Oracle queue backend probing the oracledb Python package
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
- **Merged Foundation on `main @ c8722c76`**: Chapters 1 (`364c7cbe`), 2 (`984ce732`), 3 (`80ff8a65`), 7 (`077a3fe9`), 8 (`d2fa5072`), and 9 (`abd33ab6`) are merged, establishing typed GCP Rust wheels, adapter-local primitives, shared `_fastpath_core.py`, and the MCP `2026-07-28` core with the WebMCP bridge [^fastpath-core].
- **MCP Completeness Remains Open**: Chapter 7 delivered the transport-agnostic MCP core, ASGI/WSGI/native adapters, and WebMCP projection; trusted agent context, confirmation tickets, version negotiation, subscriptions, the skills extension, Apps UI metadata, Litestar delegation parity, and the MCP CLI belong to `mcp-protocol-completeness`.
- **Open Child Flows**: `server-runtime-correctness`, `gcp-embeddings-and-identity`, and `mcp-protocol-completeness` are planned; `sqlspec-adapter-parity`, `queues-tasks-and-channels`, and `sqlalchemy-and-django-integration` remain draft charters sequenced behind sibling [native-data-platform](../native-data-platform/spec.md) driver and broker cutovers [^sqlspec-adapters].
<!-- truth: end -->

Baseline: `main @ c8722c76`. Merged squash commits: `364c7cbe` `critical-correctness-fixes`, `984ce732` `gcp-wire-protocol-correctness`, `abd33ab6` `gcp-driver-resilience` ([#73](https://github.com/cofin/vertebra/pull/73)), `f7417015` `Sync*` / `Async*` split ([#75](https://github.com/cofin/vertebra/pull/75)), `21c99f95` `db-layout-and-contract` ([#76](https://github.com/cofin/vertebra/pull/76)), `80ff8a65` `db-engine-primitives` ([#77](https://github.com/cofin/vertebra/pull/77)), `2b737240` post-merge remediation ([#80](https://github.com/cofin/vertebra/pull/80)), `d2fa5072` `runtime-structure-and-throughput` ([#92](https://github.com/cofin/vertebra/pull/92)), `077a3fe9` `mcp-webmcp-core` ([#94](https://github.com/cofin/vertebra/pull/94)), and sibling squash commits `067fa901` storage ([#101](https://github.com/cofin/vertebra/pull/101)), `af1b3d30` `driver-fail-loud` ([#102](https://github.com/cofin/vertebra/pull/102)), `1b8449fe` build packaging ([#108](https://github.com/cofin/vertebra/pull/108)), `14aa83b8` DuckDB ([#105](https://github.com/cofin/vertebra/pull/105)), `e86d8daf` MySQL ([#106](https://github.com/cofin/vertebra/pull/106)), `134546d4` SQL Server ([#107](https://github.com/cofin/vertebra/pull/107)), `47da6820` Oracle ([#103](https://github.com/cofin/vertebra/pull/103)), and `c8722c76` the `vertebra-postgres` wheel ([#104](https://github.com/cofin/vertebra/pull/104)). Verified audit findings live in [research/audit-findings_20261103.md](research/audit-findings_20261103.md).

## 1. North Star Goal & Problem Framing

### Objective
Vertebra's value is its Rust runtime and drivers. Users primarily reach them through SQLAlchemy / Advanced Alchemy, SQLSpec, Litestar, and Django. This initiative makes those integrations correct and feature-complete, keeps the public API small, and removes code that duplicates framework capabilities or relies on stand-in fakes.

### Remaining gaps
- **Drivers not yet on native Rust I/O** (tracked one PR per engine in sibling PRD [native-data-platform](../native-data-platform/spec.md)):
  - PostgreSQL: the `vertebra-postgres` wheel is on `main`, while `SyncPostgreSQLDriver` / `AsyncPostgreSQLDriver` execute through `psycopg_pool` until the `native-postgres-driver` cutover.
  - SQLite executes through Python `sqlite3` (`native-sqlite-driver`); Redis/Valkey execute over Python sockets and `vertebra_db::resp3` bindings (`native-redis-driver`).
  - MySQL, SQL Server, Oracle, DuckDB, BigQuery, and Spanner execute through their standalone wheels; `driver-conformance-ci` adds live CI proof for the merged wheels.
- **SQLSpec adapters** [^sqlspec-adapters].
  - Vertebra stores do not subclass upstream SQLSpec store bases, and schemas drift.
  - `BaseSyncDriver` / `BaseAsyncDriver` override `execute`/`select`, bypassing SQLSpec's dispatch contract and dropping `**kwargs`.
  - Six engines return `None` from `data_dictionary` (only `BigQuerySyncDataDictionary` / `BigQueryAsyncDataDictionary` and `SpannerSyncDataDictionary` / `SpannerAsyncDataDictionary` exist).
  - Upstream resolves stores by `sqlspec.adapters.*` module prefix ([sqlspec#876](https://github.com/litestar-org/sqlspec/issues/876)).
- **Queues.**
  - PostgreSQL, Oracle, and Spanner first-party queue backends require an injected `executor=`; omitting it raises `MissingDependencyError` or `ImproperConfigurationError`. Live `PostgresQueueStore` / `OracleQueueStore` executors and the `litestar-queues` module-prefix lookup constraint ([litestar-queues#186](https://github.com/cofin/litestar-queues/issues/186)) belong to `queues-tasks-and-channels`.
  - `OracleQueueBackend` probes the `oracledb` Python package when `executor=` is omitted, so its install hint names `oracledb` instead of `vertebra-core[oracle]` [^queue-oracle]; `PostgreSQLQueueBackend` probes `psycopg` until the `native-postgres-driver` cutover replaces the probe with `vertebra._postgres`.
  - `RedisQueueBackend` and `ValkeyQueueBackend` build `redis.asyncio` clients from `redis://` / `rediss://` URLs and reject `valkey://`, `valkeys://`, and `unix://` without an injected `client=` [^queue-redis]; `native-redis-driver` moves them onto `vertebra._redis`.
- **SQLAlchemy / Advanced Alchemy & Django database backends.**
  - `extensions/sqlalchemy/dialects.py::SyncDBAPIConnection` opens a second Python `sqlite3` connection beside the registry driver for SQLite URLs without a broker (so `:memory:` reads and writes land in two separate databases), defaults to `sqlite+vtb:///:memory:` when no URL is given, and selects autocommit and transaction behaviour through `isinstance` checks against `BigQueryConfig`, `SpannerConfig`, and `SQLiteConfig` [^sa-dbapi]. `broker-pool-sharing` extracts the module into `vertebra.db.dbapi`; `sqlalchemy-and-django-integration` removes the second connection and the concrete-class dispatch.
  - Relational and cloud dialects await full SQLAlchemy / Advanced Alchemy cache and Django `ENGINE` parity (`sqlalchemy-and-django-integration`).
- **Server runtime.** Mounted ASGI and RSGI responses buffer across the in-process bridge and shared-memory transports, lifespan failures do not abort worker startup, and the core wheel ships no relocatable `vertebra-server` binary (`server-runtime-correctness`).
- **Embeddings and identity.** The Vertex AI and Gemini embedding transports use incorrect wire contracts and do not send real multimodal media, and no cached audience-bound OIDC ID token minting exists for Rust and Python callers (`gcp-embeddings-and-identity`).

### What is new and why it succeeds
- Each driver package (`vertebra/db/adapters/<engine>/`) talks to its real database through its per-engine Rust extension wheel `vertebra._<engine>` ([native-data-platform](../native-data-platform/spec.md)) and exposes explicit engine-local primitives (`SyncPipelineCapable` / `AsyncPipelineCapable`, `SyncChangeSource` / `AsyncChangeSource`, `SyncNotificationSource` / `AsyncNotificationSource`, async `QueueStore`, `EmbeddingPushdown`).
- Framework extensions (`vertebra.extensions.*`, `vertebra.framework.*`, `vertebra.agent.*`) consume those protocols and implement each framework's native contract without upward imports from `vertebra.db` [^fastpath-core].

### Success metrics
- Zero production drivers open SQLite for non-SQLite engines or serve `memory://` fake rows; every engine passes `test_driver_conformance.py` via `pytest-databases`.
- `driver.pipeline()` batches N statements in one round trip on PostgreSQL and runs sequentially behind the same API on Oracle until upstream 26ai pipelining ships.
- Spanner and BigQuery Storage run in standalone `vertebra-spanner` (`vertebra._spanner`) and `vertebra-bigquery` (`vertebra._bigquery`) wheels on `google-cloud-spanner` / `google-cloud-bigquery` with `Sync*`/`Async*` client pairs and typed `GoogleCloudError` + `DatabaseError` exceptions.
- Vertebra SQLSpec adapters pass upstream ADK, events, and Litestar store contracts.
- First-party queues, `litestar-queues`, and Django tasks deliver across processes on PostgreSQL and Oracle with DI-injected dependencies, and no queue backend imports a third-party Python database or cache client.
- AST import and engine-purity gates verify `vertebra.db` imports nothing from `vertebra.{agent,framework,extensions,server,embeddings}` and generic `vertebra/db/*` modules contain zero engine leaks.
- MCP `2026-07-28` conformance passes across Starlette, FastAPI, Flask, Django, Litestar delegation, and native `App`: Chapter 7 delivered the core and adapters; `mcp-protocol-completeness` delivers the remaining protocol surface and Litestar delegation parity.
- Mounted ASGI/RSGI responses stream incrementally and `vtb run` starts from an installed core wheel (`server-runtime-correctness`).

### Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Upstream sqlspec#876 / litestar-queues#186 land late | Temporary resolver shim in `extensions/sqlspec/_compat.py`, guarded by a pinning test that fails once upstream ships so the shim is removed |
| Clean-break renames affect callers | Pre-1.0 clean-break policy (zero compatibility shims); document renames in docs and changelog |
| Emulator feature gaps (Spanner multiplexed sessions, BigQuery Storage Write) | Emulator-only gaps marked `xfail(strict=True)` with reason; behavior verified via official crates' `from_stub` seams |
| Real driver cutovers surface latent assumptions | Switch over one engine flow at a time against `test_driver_conformance.py` and `pytest-databases` |
| Archive prose drifts from code after squash merges | Plans cite squash commits on `main` and verify every cited symbol against HEAD before refinement |

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
   - Engine-agnostic code stays directly under `vertebra/db/` (`driver/`, `registry.py`, `arrow.py`, `cdc.py`, `rls.py`, `dialer/`, and framework-neutral `dbapi/` extracted by sibling `broker-pool-sharing`).
   - The adapter foundation belongs to [native-data-platform](../native-data-platform/spec.md) Chapter 1 (`db-layout-and-contract`); the neutral DB-API extraction belongs to Chapter 3 (`broker-pool-sharing`).
3. **Driver IO & `Sync*` / `Async*` split** *(user decision: required, feature complete)*.
   - Vertebra's own Rust drivers back every engine (`native-data-platform`) with explicit `Sync*` and `Async*` client/driver/capability classes (including `SyncNotificationSource` / `AsyncNotificationSource`, split in `queues-tasks-and-channels`), any Advanced Alchemy / SQLAlchemy or SQLSpec config shares one broker-owned pool over SHM (`broker-pool-sharing`), and `SyncObjectStore` / `AsyncObjectStore` are complete across frameworks (`storage-completeness`). `QueueStore` stays a single unprefixed `async` protocol because every method is asynchronous and no synchronous consumer exists.
4. **Native pipelining.**
   - `driver.pipeline()` (`SyncPipelineCapable` / `AsyncPipelineCapable`) is implemented in `vertebra-postgres` (single-`Sync` extended-protocol batch) and sequentially in `vertebra-oracle` until upstream 26ai pipelining lands.
   - `execute_stack` dispatches via `isinstance(self.native_driver, SyncPipelineCapable)` / `isinstance(self.native_driver, AsyncPipelineCapable)`; the CTE query pipeline is deleted.
5. **Registry polymorphism** [^db-registry].
   - `BaseDatabaseConfig` declares `url_schemes`, `driver_module`, `sync_driver_name`, `async_driver_name`, `from_url`, `pool_key`, and `build_sync_driver`/`build_async_driver`; `DatabaseRegistry` is table-driven (including `@overload`ed `get_dialer(config, *, sync=False)`); `provide_driver(name_or_config, *, sync=False, registry=None)` provides generic driver resolution ([database foundation](../../knowledge/domains/data/database-drivers-and-dialers.md)).
   - Sibling `broker-pool-sharing` owns the first DB-API extraction directly into `vertebra.db.dbapi` (`SyncDBAPI*` / `AsyncDBAPI*`), consuming generic registry/driver/lease contracts without engine dispatch or fallback databases. SQLAlchemy async wrapping stays in `extensions/sqlalchemy/async_adapter.py`; SQLAlchemy/Django integration consumes and hardens the same neutral package without an intermediate extension-owned DB-API.
6. **SQLSpec adapter parity** [^sqlspec-adapters].
   - Layout: `extensions/sqlspec/adapters/<engine>/{config,driver,core,data_dictionary,adk/,events/,litestar/}.py`, subclassing upstream bases and schemas with `adapter_name: ClassVar[str]` and preserving upstream SQLSpec `<Engine>SyncConfig` / `<Engine>AsyncConfig` and `<Engine>SyncDriver` / `<Engine>AsyncDriver` infix naming inside `vertebra.extensions.sqlspec`.
7. **Queues**.
   - Native `db/adapters/{postgres,oracle}/queue.py` (`PostgresQueueStore`, `OracleQueueStore`) implement the `QueueStore` protocol in `db/driver/capabilities.py` and back first-party `QueueService`, `litestar-queues`, and Django `django.tasks`, resolving dependencies through `framework/di.py::Container` (naming and location recommended in [queues-tasks-and-channels Open Decisions](../queues-tasks-and-channels/spec.md)).
8. **MCP** *(core completed in Chapter 7 `mcp-webmcp-core` [`077a3fe9`]; remaining protocol work in `mcp-protocol-completeness`)*.
   - One frozen msgspec `ToolSpec` and agent-owned `ToolRegistry` consume `vertebra.typing.ToolProvider`; transport-agnostic `MCPCore.handle()`, thin ASGI/WSGI/native adapters, `document.modelContext` WebMCP projection, and `litestar-mcp` delegation are on `main`.
   - `mcp-protocol-completeness` adds trusted `AgentContext` propagation, single-pass guards and single-use confirmation tickets, dual-mode version negotiation with legacy sessions and `GET` SSE streams, resource subscriptions, the skills extension, MCP Apps UI metadata, Litestar delegation parity, and the `vtb ai mcp serve` / `bridge` commands with OIDC identity.
9. **Rust GCP clients** *(completed in Chapters 2 and 9, extracted to per-driver wheels in `db-layout-and-contract`)*.
   - Spanner and BigQuery Storage Read/Write run on `google-cloud-spanner =0.34.8-preview` (`vertebra-spanner`) and `google-cloud-bigquery =0.19.0` (`vertebra-bigquery`) (`rustls` + `ring`) bridged to `SyncGcpTokenCache` / `AsyncGcpTokenCache`, with panic/cancel-safe async completion over `AsyncCompletionQueue` and typed `GoogleCloudError` + `DatabaseError` hierarchy.

### Alternatives rejected
- **Per-feature subpackages (`db/cdc/{postgres,spanner}.py`).** Splits one engine across multiple packages and loses locality with its driver.
- **Monolithic `vertebra._vertebra` linking every database SDK.** Pulls gRPC, wire, and C++ dependencies into `vertebra-core` for users who only install one engine or HTTP-only Vertebra.
- **Dual-mode classes mixing sync and `*_async` methods.** Replaced codebase-wide with explicit `Sync*` and `Async*` classes exposing plain method names.
- **Deleting Vertebra SQLSpec stores in favor of upstream-only.** Feature-complete Vertebra adapters are required.
- **Native stores without upstream bases.** Causes schema drift and breaks `SQLSpecSessionService` and extension migrations.
- **Keeping the CTE query pipeline alongside `driver.pipeline()`.** A CTE wrapper is not wire pipelining, cannot mix arbitrary DML safely, and duplicates SQLSpec `StatementStack`.
- **One MCP core including Litestar.** Duplicates `litestar-mcp`; Litestar apps delegate to it, and `mcp-protocol-completeness` closes the remaining delegation parity gaps.

## 3. Master Roadmap (Ordered Child Flows)

| Ch | Flow ID | Title | Key Deliverables | Public Seams | Depends on | State |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `critical-correctness-fixes` | Silent failure and security fixes | Confirmation-gate binding, opt-in WebMCP, prefix-scoped `SCAN`+`UNLINK`, fail-loud embeddings + `ORDER BY idx`, ASGI batch timeout | existing APIs | — | completed (`364c7cbe`) |
| 2 | `gcp-wire-protocol-correctness` | Official Google Cloud Rust clients for Spanner and BigQuery Storage | `google-cloud-spanner` / `google-cloud-bigquery` migration, commit/DDL/append verification, hardened `ArrowAppendPlan` | `SyncSpannerClient`/`AsyncSpannerClient`, `SyncBigQueryClient`/`AsyncBigQueryClient` | — | completed (`984ce732`) |
| 3 | `db-engine-primitives` | Engine primitives relocation and CTE pipeline removal | CDC/notify/change-stream/embedding pushdown in `db/adapters/<engine>`, `ChannelBus` decoupled, `SyncPipelineCapable`/`AsyncPipelineCapable` dispatch, adapter-owned GCP ToolProviders, neutral typing contract, dialect-agnostic `db/rls.py` | `vertebra.db.cdc`, `vertebra.db.adapters.*.{notify,change_stream,embeddings}`, `vertebra.typing.ToolProvider` | 1, 2, 9; native-data-platform `db-layout-and-contract` | completed (`80ff8a65`) |
| 4 | `sqlspec-adapter-parity` | Feature-complete SQLSpec adapters | Per-engine adapter packages, dispatch-contract drivers, upstream-base stores, `Sync`/`Async` data dictionaries, `_compat.py` resolver shim, shared-pool refcount, `VTBARROW` broker results | `<Engine>{Sync,Async}Config`, `adapter_name` | native-data-platform `native-postgres-driver`, `broker-pool-sharing`, `native-sqlite-driver` | draft |
| 5 | `queues-tasks-and-channels` | Working queues, tasks, channels, stores | Native `PostgresQueueStore` / `OracleQueueStore`, settings-driven `QueueService`, `litestar-queues` & Django tasks backends with DI, single cron parser, distributed Litestar/Starlette channels, `ValkeyStore`, `vertebra-core[oracle]` install hint | `QueueService`, `QueueBackendSettings`, `QueueStore` implementations | 4; native-data-platform `native-postgres-driver`, `native-redis-driver` | draft |
| 6 | `sqlalchemy-and-django-integration` | SQLAlchemy / AA / Django DB integration | `Sync<Engine>Dialect` / `Async<Engine>Dialect` + official GCP/DuckDB dialect bases over `db/dbapi/`, second-`sqlite3`-connection and concrete-class dispatch removal, SQLAlchemy-only `async_adapter.py`, AA dogpile cache backend, real Django backends including Spanner via `django_spanner` | dialect entry points, `create_cache_config`, Django `ENGINE` paths | native-data-platform `broker-pool-sharing` | draft |
| 7 | `mcp-webmcp-core` | MCP 2026-07-28 core and WebMCP bridge | `ToolSpec`/`ToolRegistry`, `MCPCore`, ASGI/WSGI/native adapters, `litestar-mcp` delegation, TS bridge on `document.modelContext` | `ToolSpec`, `MCPCore`, `registerWebMCPTools` | 1, 3 | completed (`077a3fe9`) |
| 8 | `runtime-structure-and-throughput` | Fastpath, Rust modules, helpers | Shared `_fastpath_core.py` + `FastPathProvider`, Rust submodule splits + shared `py_async`, zero-copy Arrow append, `VertexEmbedder` HTTP/2 transport, `utils` fallback removal | `FastPathProvider`, `vertebra.utils.*`, `_vertebra.pyi` | 2, 3, 9 | completed (`d2fa5072`) |
| 9 | `gcp-driver-resilience` | GCP driver resilience and typed errors | Panic-safe/cancellable `spawn_rust_db_task`, typed `GoogleCloudError` hierarchy, async `gcloud` + `ProxyPolicy` + explicit scopes | `_vertebra.pyi` exceptions, `TaskPanicError`, `{Sync,Async}GcpTokenCache.from_options` | 2 | completed (`abd33ab6`) |
| 10 | `server-runtime-correctness` | Server runtime correctness | Incremental ASGI/RSGI streaming across bridge and SHM transports, fail-loud lifespan startup, exported supervisor API, real worker-only and worker lanes, and a relocatable `vertebra-server` binary in every core wheel with a `vtb run` smoke test ([spec.md](../server-runtime-correctness/spec.md)) | server runner and supervisor APIs | — | planned |
| 11 | `gcp-embeddings-and-identity` | GCP embedding transports and identity tokens | Correct Vertex AI and Gemini embedding wire contracts, real multimodal media, hardened embedder auth and micro-batching, standardized ADC resolution, cached audience-bound OIDC ID token minting for Rust and Python ([spec.md](../gcp-embeddings-and-identity/spec.md)) | embedder classes, ID token API | — | planned |
| 12 | `mcp-protocol-completeness` | MCP protocol completeness | Trusted `AgentContext` propagation, single-use confirmation tickets, dual-mode version negotiation with legacy sessions and `GET` SSE, resource subscriptions, skills extension, MCP Apps UI metadata, Litestar delegation parity, `vtb ai mcp serve` / `bridge` with OIDC identity ([spec.md](../mcp-protocol-completeness/spec.md)) | `MCPCore`, MCP CLI commands | 10, 11 | planned |

## 4. Global Constraints (Applicable to All Child Flows)

- **Branching.** Never commit or push directly to `main`; each flow lands on a dedicated feature/fix branch with signed conventional commits.
- **Clean break & `Sync*` / `Async*` naming.**
  - Leading `Sync*` and `Async*` prefixes on all dual-mode Rust and Python classes (`SyncCompletionQueue` / `AsyncCompletionQueue`, `Sync<Engine>Client` / `Async<Engine>Client`, `SyncObjectStore` / `AsyncObjectStore`, `Sync<Engine>Dialect` / `Async<Engine>Dialect`), with plain un-suffixed method names and zero mixed `*_async` methods (preserving upstream `sqlspec` `<Engine>SyncConfig` / `<Engine>AsyncConfig` infix naming in `vertebra.extensions.sqlspec` and required dual-mode external protocol adapters).
  - Zero backward-compatibility aliases, re-export shims, or one-time upgrade flags.
  - Zero `.get()`/`try-except` fallbacks added for synthetic test seeds; update tests to match the production contract.
  - Zero emulator hooks or in-memory/SQLite fallbacks in production driver classes.
- **Python style.**
  - `msgspec.Struct` value objects over `@dataclass`, with `frozen=True` / `gc=False` / `array_like=True` applied only where their invariants hold, and inline field docstrings.
  - PEP 585/604/695 types, Google docstrings, zero inline `#` comments, top-level imports (`TYPE_CHECKING` / cached `module_loader` helpers for optional deps), class layout (`attributes -> __init__ -> properties -> public -> private`), centralized `vertebra.utils.serializers`, and `@pytest.mark.parametrize` / parameterized fixtures instead of test loops.
- **Public helpers.** `vertebra.utils.*` functions remain public user helpers regardless of in-repo caller count.
- **Small API surface.** Add public symbols only where a framework or capability contract requires them; Advanced Alchemy / SQLAlchemy remains the primary user path.
- **Tests.** Place tests in existing canonical module test files (never standalone `test_*remediation*.py` or `test_*audit*.py`).
- **Docs and knowledge.** Each child flow updates affected `docs/` pages and `.agents/bundles/knowledge/` chapters so `make docs` and `make review-check` stay green at every checkpoint, and records every caveat in a limitations chapter.
- **Checkpoints.** Plans cite squash commits on `main`, never feature-branch SHAs orphaned by squash merges.

## 5. Backlog (Deferred Items)

- Spanner and Redis/Valkey native `QueueStore` backends built on the PostgreSQL/Oracle pattern.
- AWS RDS/Aurora DSQL IAM dialer ([#67](https://github.com/cofin/vertebra/issues/67)) and Azure Entra ID dialer ([#68](https://github.com/cofin/vertebra/issues/68)).
- Removing `extensions/sqlspec/_compat.py` once [sqlspec#876](https://github.com/litestar-org/sqlspec/issues/876) and [litestar-queues#186](https://github.com/cofin/litestar-queues/issues/186) ship.
- Interactive server-side multi-statement Spanner `ReadWrite` session RPCs outside `run_in_transaction` (`driver.transaction()` buffers DML and mutations for atomic flush).
- Typing chore ([#59](https://github.com/cofin/vertebra/issues/59)), scheduled as a standalone typing cleanup outside the child flows.
- Standalone packaging drops `.agents/skills` ([#115](https://github.com/cofin/vertebra/issues/115)); split into its own packaging follow-up flow.
- [#116](https://github.com/cofin/vertebra/issues/116): deferred and optional; the user schedules it later.

## Implementation Plan

- [x] [364c7cbe] critical-correctness-fixes — Silent failure and security fixes
- [x] [984ce732] gcp-wire-protocol-correctness — Official Google Cloud Rust clients for Spanner and BigQuery Storage
- [x] [abd33ab6] gcp-driver-resilience — GCP driver resilience and typed errors
- [x] [80ff8a65] db-engine-primitives — Engine-owned primitives, neutral ToolProviders, native stacks, and tenant boundaries ([query-pipeline-cdc-and-tenancy.md](../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md))
- [ ] sqlspec-adapter-parity — Feature-complete SQLSpec adapters ([spec.md](../sqlspec-adapter-parity/spec.md))
- [ ] queues-tasks-and-channels — Working queues, tasks, channels, stores ([spec.md](../queues-tasks-and-channels/spec.md))
- [ ] sqlalchemy-and-django-integration — SQLAlchemy / AA / Django DB integration ([spec.md](../sqlalchemy-and-django-integration/spec.md))
- [x] [077a3fe9] mcp-webmcp-core — MCP 2026-07-28 core and WebMCP bridge
- [x] [d2fa5072] runtime-structure-and-throughput — Fastpath, Rust modules, helpers
- [ ] server-runtime-correctness — Incremental streaming, fail-loud startup, worker modes, and wheel-shipped server binary ([spec.md](../server-runtime-correctness/spec.md))
- [ ] gcp-embeddings-and-identity — Google Cloud embedding transports and audience-bound identity tokens ([spec.md](../gcp-embeddings-and-identity/spec.md))
- [ ] mcp-protocol-completeness — MCP protocol completeness ([spec.md](../mcp-protocol-completeness/spec.md))

## Continuity Snapshot

- **Flow ID:** `integration-remediation` (`state: planned`, `plan_revision: 10`, `plan_commit: null`, `last_verified_checkpoint: c8722c76`)
- **Active Focus:** Chapters 1, 2, 3, 7, 8, and 9 are merged on `main @ c8722c76`. `server-runtime-correctness` and `gcp-embeddings-and-identity` are planned with no flow dependencies; `mcp-protocol-completeness` is planned behind both. Chapters 4, 5, and 6 remain draft charters behind `native-postgres-driver`, `broker-pool-sharing`, `native-sqlite-driver`, and `native-redis-driver`.
- **Key decisions:**
  - Engine adapters and local primitives live under `vertebra/db/adapters/<engine>/` backed by per-engine PyO3 wheels `src/rs/vertebra-<engine>` (`vertebra._<engine>`); framework and agent integrations live under `vertebra.extensions.*`, `vertebra.framework.*`, and `vertebra.agent.tools`.
  - `execute_stack` dispatches on `SyncPipelineCapable` / `AsyncPipelineCapable`, using the protocols in the [database foundation](../../knowledge/domains/data/database-drivers-and-dialers.md).

[^db-registry]: `src/py/vertebra/db/registry.py::DatabaseRegistry` — Table-driven database driver and dialer registry.
[^fastpath-core]: `src/py/vertebra/server/_fastpath_core.py` — Shared `FastPathProvider` core across ASGI/WSGI/native frameworks.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — SQLSpec adapter configuration and driver bindings.
[^sa-dbapi]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection` — PEP 249 connection with a second `sqlite3` connection and concrete-config dispatch.
[^queue-redis]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — Redis and Valkey queue backends building `redis.asyncio` clients.
[^queue-oracle]: `src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend` — Oracle queue backend probing the `oracledb` Python package.
