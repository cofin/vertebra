# Changelog

All notable changes to Vertebra (`vertebra-core`) are documented in this file.

## 0.5.0

### Added

- Added runnable browser examples for server-sent live-state updates and for WebAssembly execution. [PR #165](https://github.com/cofin/vertebra/pull/165)

### Changed

- `vtb init` generates standalone route declarations and assembles them through `App(route_handlers=[...])` in both templates. The routing guide shows controllers, nested routers, and layered guards. [PR #165](https://github.com/cofin/vertebra/pull/165)
- The documentation examples expose importable apps or runnable workflows with commands and expected results. [PR #165](https://github.com/cofin/vertebra/pull/165)

### Fixed

- Native route registration keeps dependency injection and thread offload settings local to each registration, including when apps share a handler or one function declares multiple routes. [PR #165](https://github.com/cofin/vertebra/pull/165)

## 0.4.0

Performance, API-alignment, packaging, and documentation release delivering an adaptive Rust WSGI executor pool, zero-copy embedded bridge and HTTP ingress hot-path optimizations, SQLite SQLAlchemy fastpaths, lock-free per-loop async SHM broker handoff with single-waiter futex wakeups, lazy import-string mounts (`vtb run --mount`), clean-break domain naming and a config-first `vertebra.build.Bundler`, complete PyPI package metadata, and plain-English documentation with themed SVG diagrams.

### Added

- Lazy import-string application mounts (`app.mount("/path", "pkg.mod:app")`) and `vtb run --mount` CLI support ([PR #162](https://github.com/cofin/vertebra/pull/162)).
- Adaptive Rust `WsgiExecutor` thread pool with stall watchdog, batch-shared `Arc<WsgiServerConfig>`, GIL-safe shutdown, and serialized shared-memory response writes (`SHM_RESPONSE_LOCK`) ([PR #163](https://github.com/cofin/vertebra/pull/163)).
- Single-waiter SHM futex wakeup (`ShmLeaseTable::wake_one_waiter_seqcst`), `ShmLeaseTable::{wake_seq, partition_count, record_timeout, wait_for_wake}`, PyO3 `LeaseBroker::{wake_seq, partition_count, record_timeout, wait_for_wake_async}` over Tokio `spawn_blocking`, and unified FIFO per-event-loop `AsyncSHMBrokerTransport` local handoff with a single background cross-process watcher ([PR #163](https://github.com/cofin/vertebra/pull/163)).
- Lazy public bundler facade in `vertebra.build` (`Bundler`, `BundleConfig`, `BundleDevConfig`, `BundleResult`, `BundleOutput`, `SourceWatcher`, `IsolatedDeclarationsGenerator`, `minify_css`) with config-first `Bundler(config)` / `Bundler.from_config(...)` and context-managed dev-watcher teardown ([PR #163](https://github.com/cofin/vertebra/pull/163)).
- `granian-rsgi-data-stdlib` (`asyncpg`) benchmark target on port `8170`, single-checkout/single-lease and sorted `row_id` update parity across `NativePostgresWorldDriver` and `StdlibPostgresWorldDriver`, and `--loop uvloop` on non-WSGI Granian data targets ([PR #163](https://github.com/cofin/vertebra/pull/163)).
- Stdlib SVG architecture diagram generator (`tools/docs/render_diagrams.py`, wired with `--check` into `Makefile`) emitting paired light/dark SVGs (`runtime-architecture`, `circuit-breaker`, `shm-broker`, `cloud-dialers`) under `docs/assets/diagrams/` ([PR #163](https://github.com/cofin/vertebra/pull/163)).

### Changed

- Right-sized non-free-threaded Embedded Tokio runtimes (`worker_threads = 1`) and consumer-aligned `RequestBatcher` sharding, added lock-free `BatchResponseSink`, zero-copy `PathAndQuery` in `InProcessRequest`, batch-locked `BridgeCorrelation::dispatch_bridge_vec`, and owned-buffer `ResponseSubmitter::submit_batch_flushed` across ASGI, RSGI, Native, and WSGI runners ([PR #163](https://github.com/cofin/vertebra/pull/163)).
- Optimized HTTP ingress, backpressure, SHM batching, PyO3 scopes, RSGI dispatch, and worker drain paths (`IdleTimeoutStream`, connection-pinned collector shards, single-pass `encode_header_map_compact`, borrowed `AdmissionPermit<'a>`, `CLOCK_MONOTONIC_COARSE`, `ShmDispatchScratch`, bitmask-drop `AsgiScope`, lazy `RsgiHttpProtocol`, eventfd-split `_on_notify_readable`/`_drain_batches`, merged native dispatch, and specialized single-argument extractors) ([PR #163](https://github.com/cofin/vertebra/pull/163)).
- Replaced `vertebra-sqlite` naive substring checks with single-pass comment/quote/bind-parameter-aware `analyze_statement_facts` to skip implicit `BEGIN IMMEDIATE` on read-only and non-transactional control `lazy_begin` queries (preserving `SAVEPOINT`), eliminated per-statement `mpsc::channel()` allocations in `SqlitePool` via locked `WorkerSlot::with_conn`, and defaulted SQLAlchemy SQLite `+vtb`/`+vtb_async` dialects to `max_readers=0` (honoring case-insensitive `?max_readers=` URL overrides and `connect_args`) ([PR #163](https://github.com/cofin/vertebra/pull/163)).
- Aligned public domain class names: renamed internal PyO3 awaitables to `EmptyAwaitable`, `ValueAwaitable`, and `TaskAwaitable`; replaced `SpecTree`/`SpecNode` with `PlanTree`/`PlanNode` in `vertebra.agent.planning`; moved `Agent`/`AgentGroup`/`AgentMessage` to `vertebra.agent.agents`; renamed `DynamicWorkflow`/`WorkflowNode`/`StepResult` to `Workflow`/`WorkflowStep`/`WorkflowStepResult`; standardized on protocol-neutral `ToolRegistry`, `BaseKeyValueStore`, `JSONRPCException`, normalized MCP/A2A/Email acronym casing, and `Sync*`/`Async*` symmetry (`SyncTenantConnectionScope`, `SyncBrokerStream`, `SyncBrokerPool`, `AsyncPostgresNotificationDriver`, `AsyncRESPStreamClient`); and removed `agent/spec.py`, unused `agent/config.py`, and stream-client aliases ([PR #163](https://github.com/cofin/vertebra/pull/163)).
- Published complete PyPI metadata across all 13 packages with `check_package_metadata.py`, hardened release workflows, overhauled `README.md` and `docs/` prose into plain English, and replaced linear Mermaid diagrams with themed SVGs ([PR #159](https://github.com/cofin/vertebra/pull/159), [PR #160](https://github.com/cofin/vertebra/pull/160), [PR #162](https://github.com/cofin/vertebra/pull/162), [PR #163](https://github.com/cofin/vertebra/pull/163)).
- Upgraded Python, Rust, TypeScript, and pre-commit dependencies via `make upgrade` (`advanced-alchemy 1.13.0b4`, `sqlspec 0.74.2`, `sqlalchemy 2.1.9`, `django 6.1.3`, `fastapi 0.165.0`, `granian 2.8.9`, `wasmtime 51.0.0`, `ruff 0.17.6`, `vite 8.4.0`, `vitest 5.1.2`, `@biomejs/biome 2.6.9`) ([PR #163](https://github.com/cofin/vertebra/pull/163)).

### Fixed

- Fixed `AsyncSHMBrokerTransport` lock re-entrancy deadlock under `asyncio.eager_task_factory` by spawning `_watch_broker_wakes` outside `_waiter_lock`, eliminated closed-loop retention in `WeakKeyDictionary`, bounded `local_wakes` credit against `watcher_seq` to prevent missed cross-process wakeups, skipped closed event loops in `_on_slot_freed`, paired `register_partition` `partition_count` increments with `Ordering::Release`, and preserved `total_timeouts` accounting for async timeouts ([PR #163](https://github.com/cofin/vertebra/pull/163)).
- Fixed SQLite WAL connection opening order (`busy_timeout` before checking journal mode, skipping redundant `PRAGMA journal_mode = WAL`) and kept `SqlitePool.in_transaction` synchronized strictly under the writer lock across raw SQL and pre-cancelled `commit`/`rollback` operations ([PR #163](https://github.com/cofin/vertebra/pull/163)).
- Fixed `PlanTree` leaf readiness, duplicate `ToolRegistry` exports, async SHM broker release deadlocks, PostgreSQL benchmark advisory seed locking, and optional `aiosqlite` benchmark gating ([PR #163](https://github.com/cofin/vertebra/pull/163)).

## 0.3.0

Release introducing 13 standalone Rust PyO3 database, storage, and bundler wheels, shared-memory connection broker with C-ABI capsules and standalone PEP 249 DB-API, QUIC HTTP/3 and progressive response streaming, shared-memory vector embeddings, active Google Cloud/AWS/Azure database dialers, MCP 2026-07-28 dual-mode sessions, `pyarrow`-free `ArrowCursor` C streams, integrated multi-protocol benchmarks, and cross-platform Windows/macOS release packaging.

### Added

- Standalone optional Rust PyO3 native driver wheels across all major engines: `vertebra-postgres` (including logical replication and PG14–18 conformance), `vertebra-oracle`, `vertebra-mssql`, `vertebra-mysql`, `vertebra-duckdb`, `vertebra-sqlite`, `vertebra-redis`, and `vertebra-valkey`, alongside `vertebra-bigquery` and `vertebra-spanner` ([PR #71](https://github.com/cofin/vertebra/pull/71), [PR #103](https://github.com/cofin/vertebra/pull/103), [PR #104](https://github.com/cofin/vertebra/pull/104), [PR #105](https://github.com/cofin/vertebra/pull/105), [PR #106](https://github.com/cofin/vertebra/pull/106), [PR #107](https://github.com/cofin/vertebra/pull/107), [PR #134](https://github.com/cofin/vertebra/pull/134), [PR #143](https://github.com/cofin/vertebra/pull/143)).
- Cross-process shared-memory connection broker with `BrokerEngineCapsule` C-ABI vtables, `0600` Unix domain socket `SCM_RIGHTS` attach (`BrokerAttachClient`/`BrokerAttachServer`), lease quarantine/reclamation, and standalone PEP 249 DB-API 2.0 wrappers (`vertebra.db.dbapi`) ([PR #141](https://github.com/cofin/vertebra/pull/141)).
- QUIC HTTP/3 listener (`--features http3` fail-loud gates), graceful shutdown drain, progressive HTTP/SSE/WebSocket response streaming, relocatable `vertebra-server` wheel binary, and event-loop diagnostics (`LoopBackend` across `uvloop`/`zuvloop`/`rloop`) ([PR #65](https://github.com/cofin/vertebra/pull/65), [PR #121](https://github.com/cofin/vertebra/pull/121), [PR #138](https://github.com/cofin/vertebra/pull/138), [PR #146](https://github.com/cofin/vertebra/pull/146)).
- Multicloud object storage plane (`vertebra-storage`) with content-integrity receipts, verified reads, streaming Arrow writes, bulk transfers, and wheel isolation ([PR #101](https://github.com/cofin/vertebra/pull/101), [PR #126](https://github.com/cofin/vertebra/pull/126)).
- `pyarrow`-free `ArrowCursor` C-stream exports, AWS SigV4 (RDS/Aurora DSQL) and Azure Entra database dialers with retry logic, OIDC ID token cache (`SyncGcpIdTokenCache`/`AsyncGcpIdTokenCache`), HTTP/2 Vertex transport, and OKF Dataplex Knowledge Catalog sync ([PR #92](https://github.com/cofin/vertebra/pull/92), [PR #132](https://github.com/cofin/vertebra/pull/132), [PR #146](https://github.com/cofin/vertebra/pull/146), [PR #148](https://github.com/cofin/vertebra/pull/148)).
- MCP 2026-07-28 dual-mode sessions, SSE subscriptions, skills/UI metadata, WebMCP bridge, and CLI bridge ([PR #94](https://github.com/cofin/vertebra/pull/94), [PR #133](https://github.com/cofin/vertebra/pull/133)).
- Integrated multi-protocol and standalone adapter benchmark matrix (`tools.benchmarks` with DuckDB store, `oha`/`h2load`/`ghz`/`k6` drivers, RustFS/Floci/Azurite fixtures) and live zero-skip database conformance lanes ([PR #135](https://github.com/cofin/vertebra/pull/135), [PR #147](https://github.com/cofin/vertebra/pull/147)).
- Shared-memory vector storage and SIMD kernels in `vertebra-core` (`VectorSlab` with `VTBVSLAB` header, set-associative seqlock `VectorCache`, `SimilarityMatch`, `StaticEmbedderCore`, Matryoshka Representation Learning `mrl_truncate_and_normalize_f32`, `quantize_f32_to_int8`, `quantize_f32_to_binary`, and SIMD cosine/dot/L2/Hamming `top_k_*` search) ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Zero-copy PEP 3118 buffer protocol types `VectorView` and `VectorBatchView` in `vertebra._vertebra` with `.truncate()`, `.quantize("int8" | "binary")`, `.top_k()`, `.to_pgvector_bytes()`, `.to_oracle_bytes()`, `.to_sqlite_blob()`, and `.to_arrow_shm()` ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Binary database vector codecs for PostgreSQL `pgvector` (`encode_pgvector_f32`, `decode_pgvector_f32`, `encode_pgvector_f16`, `decode_pgvector_f16`, `encode_pgvector_bit`, `decode_pgvector_bit`), Oracle 26ai `VECTOR` (`0xDB` binary wire format via `encode_oracle_vector_f32`, `decode_oracle_vector_f32`, `encode_oracle_vector_int8`, `encode_oracle_vector_binary`), and SQLAlchemy `Vector` type bind/result pushdown ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Native HTTP/2 gRPC 5-byte Length-Prefixed Message (LPM) packed-`f32` protobuf framing (`encode_gemini_embed_grpc_frame`, `decode_gemini_embed_grpc_frame`, `decode_gemini_embed_grpc_frame_to_batch`) in `vertebra-http` and `vertebra._vertebra` ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Google-first, SQL pushdown, and local shared-memory embedding providers in `vertebra.embeddings`: `Embedder`, `VertexEmbedder`, `GeminiEmbedder`, `PushdownEmbedder` (wrapping adapter-owned `BigQueryEmbeddingPushdown`, `SpannerEmbeddingPushdown`, `PostgresEmbeddingPushdown`, and `OracleEmbeddingPushdown`), `RemoteEmbedder`, `create_embedding_grpc_service`, `GemmaEmbedder` (with `fp16` activation overflow guard), `FastEmbedder`, `StaticEmbedder`, `EmbeddingBroker` (single `1x` RAM model hosting across workers, `BackendFactory`, token-budgeted length-sorted dynamic batching, and `3.14t`/`3.15t` subprocess GIL quarantine), `EmbeddingError`, and `ModelSpec` catalog ([PR #64](https://github.com/cofin/vertebra/pull/64), [PR #75](https://github.com/cofin/vertebra/pull/75), [PR #76](https://github.com/cofin/vertebra/pull/76)).
- Active Google Cloud Application Default Credentials (ADC) token cache and refresh provider (`GcpTokenSnapshot`, `GcpAuthSource`, `SyncGcpTokenCache`, `AsyncGcpTokenCache`, `RefreshingGcpCredentialProvider`) in `vertebra-core`, `vertebra-storage`, and `vertebra.storage` with explicit OAuth scopes, proxy environment support, `300s` proactive refresh skew, single-flight refresh deduplication, generation-checked `401` invalidation, and automatic DuckDB `CREATE OR REPLACE SECRET` token rotation in `SyncDuckDBDriver` and `AsyncDuckDBDriver` ([PR #64](https://github.com/cofin/vertebra/pull/64), [PR #77](https://github.com/cofin/vertebra/pull/77)).
- Standalone optional Rust PyO3 Google Cloud database wheels `vertebra-bigquery` (`vertebra._bigquery`, `[bigquery]`) and `vertebra-spanner` (`vertebra._spanner`, `[spanner]`) powered by official `google-cloud-bigquery` and `google-cloud-spanner` crates, zero-copy `ShmCapsule` `VTBARROW` cursors, `SpannerConfig.admin_endpoint` REST DDL, `ca_pem` custom CA support, and multi-base exceptions (`GoogleCloudError`, `GoogleCloudAuthError`, `SpannerError`, `SpannerAbortedError`, `BigQueryError`, `BigQueryRowError`, `TaskPanicError`) ([PR #71](https://github.com/cofin/vertebra/pull/71), [PR #77](https://github.com/cofin/vertebra/pull/77)).
- Per-engine database adapter packages under `vertebra.db.adapters.<engine>`, native statement pipelining (`SyncPipelineCapable`, `AsyncPipelineCapable`, `driver.pipeline()`), adapter-owned `ToolProvider` implementations (`BigQueryToolProvider`, `SpannerToolProvider`) consumed via `vertebra.agent.tools.build_tools`, `SpannerQueueBackend`, and `ChannelBus` ([PR #71](https://github.com/cofin/vertebra/pull/71), [PR #72](https://github.com/cofin/vertebra/pull/72), [PR #73](https://github.com/cofin/vertebra/pull/73), [PR #76](https://github.com/cofin/vertebra/pull/76)).
- Consolidated `vtb ai` CLI group (`vertebra.cli.ai`) providing `vtb ai models list|pull|stage|warm` and `vtb ai mcp serve|bridge` ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Co-located background `WorkerRunner` execution in `vtb run` and `vtb dev` via `--with-workers`, `--queues`, `--lane-concurrency`, and `[tool.vertebra.workers]` in `pyproject.toml`, plus `--oneshot` and `--payload` in `vtb workers run` (`execute_worker_cli`) ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Automatic default `AgentInterface` synthesis (`protocol_binding="JSONRPC"`, `protocol_version="1.0"`) on `AgentCard` and dynamic request-origin URL resolution (`Forwarded`, `X-Forwarded-Proto`, `X-Forwarded-Host`, `Host` with `Vary` response headers) in `A2AHttpHandler` and `A2AService` ([PR #64](https://github.com/cofin/vertebra/pull/64)).

### Changed

- Replaced blocking `runtime().block_on(...)` calls in `vertebra-storage` async PyO3 operations with non-blocking Tokio `spawn_rust_task` execution (`TaskCompletionCallback`) ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Replaced top-level `vtb mcp` CLI command group with `vtb ai mcp` (`serve` and `bridge`) ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Made browser WebMCP exposure opt-in per tool (`mcp_tool(webmcp=True)`, `Tool(webmcp=True)`), disabled `VertebraConfig.swap_memory_channels` and `swap_memory_response_cache` by default, replaced `TenantScopeConfig.guc_name` with `setting_name` plus adapter-owned session SQL, and scoped Redis/Valkey key-value store clearing to `SCAN` + `UNLINK` under `key_prefix` ([PR #75](https://github.com/cofin/vertebra/pull/75), [PR #76](https://github.com/cofin/vertebra/pull/76)).
- Updated brand logo and icon assets (`docs/assets/`) to an isometric axial vertebrae mark (`#EDB641`), pinned persistent `VERTEBRA / <Current Page>` header context in `docs/stylesheets/extra.css`, and split usage documentation into 14 focused topic guides under `docs/usage/` ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Updated `.github/dependabot.yml` to use native `bun` and `uv` package ecosystems with grouped minor/patch updates and `open-pull-requests-limit: 1`, gated CI test matrices on `lint` with `fail-fast: true` in `.github/workflows/ci.yml`, and upgraded workspace dependencies via `make upgrade` (`anyio`, `bump-my-version`, `platformdirs`, `pytest-databases`, `python-dateutil`, `sqlparse`, `crc32fast`) ([PR #64](https://github.com/cofin/vertebra/pull/64)).

### Fixed

- Fixed `dorny/paths-filter@v4` shallow-clone `git fetch` failure on `push` events to `main` by setting `fetch-depth: 2` in `.github/workflows/ci.yml`, and removed failing `git push` commit step from `.github/workflows/benchmark.yml` ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Scoped `[tool.bumpversion.files]` search/replace in `pyproject.toml` to `version = "{current_version}"` so `make release` does not rewrite dependency version floors, and automated `uv.lock` and `Cargo.lock` synchronization in `Makefile` `release`/`pre-release` targets ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Enforced `ConfirmationGate` caller and argument hash binding across `MCPService`, `AgentRuntime`, and `vertebra.agent.tools.build_tools`, hardened `EventLoopPool.stop` and `VERTEBRA_ASGI_BATCH_TIMEOUT` (`504 Gateway Timeout`), and isolated optional native wheel manifests (`vertebra-bundler`, `vertebra-storage`, `vertebra-bigquery`, `vertebra-spanner`) ([PR #75](https://github.com/cofin/vertebra/pull/75), [PR #76](https://github.com/cofin/vertebra/pull/76), [PR #77](https://github.com/cofin/vertebra/pull/77)).
- Removed silent in-memory database driver fallbacks, consolidated duplicated driver/extension code onto `sqlspec 0.74.1` and `advanced-alchemy 1.13.0b3`, repaired release wheel packaging (`delocate-wheel` `@rpath` on macOS, `delvewheel` namespace packaging and memory queue default on Windows MSVC), and cleared Windows Rust/Python warnings ([PR #102](https://github.com/cofin/vertebra/pull/102), [PR #144](https://github.com/cofin/vertebra/pull/144), [PR #153](https://github.com/cofin/vertebra/pull/153), [PR #155](https://github.com/cofin/vertebra/pull/155), [PR #156](https://github.com/cofin/vertebra/pull/156), [PR #157](https://github.com/cofin/vertebra/pull/157), [PR #158](https://github.com/cofin/vertebra/pull/158)).

## 0.2.0

Comprehensive release introducing zero-copy serialization bridges, real-time database channels, Advanced Alchemy object storage and cache services, bidirectional Protobuf/gRPC tooling, multi-framework extensions, OpenAPI documentation portals, WebMCP auto-registration, and modernized repository security tooling.

### Added

- Zero-copy C-API `msgspec` bridge and vectorized row serialization via `Request.decode_msgspec`, `WsgiInput.decode_msgspec`, `AsgiScope.decode_msgspec`, `hydrate_rows_vectorcall`, and thread-local scratch arenas in `_zero_copy.py` ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- Direct struct-to-wire response serialization via `encode_msgspec_response` ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- Real-time database-backed channels via `DatabaseChannelDriver`, `DatabaseChannelConfig`, `DatabaseChannelSubscription`, and `DatabaseChannelPublisher` supporting PostgreSQL `LISTEN`/`NOTIFY`, MySQL advisory locks, SQLite WAL triggers, and message queuing ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- Advanced Alchemy object storage and async cache integrations via `ObjectStorageBackend`, `ObjectStorageConfig`, `ObjectStorageFileService`, `AsyncCacheManager`, `AsyncCacheConfig`, and `ZeroCopyServiceMixin` ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- Bidirectional Protobuf/gRPC tooling and Length-Prefixed Message (LPM) framing via `ProtobufParser`, `ProtobufPythonGenerator`, `ProtobufTypeScriptGenerator`, `ProtobufGenerator.from_routes`, `encode_lpm_frame`, and `decode_lpm_frame` ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- Multi-framework extensions for Starlette (`vertebra.extensions.starlette.StarletteExtension`), FastAPI (`vertebra.extensions.fastapi.FastAPIExtension`), Flask (`vertebra.extensions.flask.FlaskExtension`), and Django (`vertebra.extensions.django.DjangoExtension`) ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- Six native Django database backends with Arrow acceleration via `DatabaseWrapperMixin`, `ArrowQuerySetMixin`, `VertebraPostgresBackend`, `VertebraMysqlBackend`, `VertebraSqliteBackend`, `VertebraOracleBackend`, `VertebraMssqlBackend`, and `VertebraDuckdbBackend` ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- Unified interactive API documentation portals via `OpenAPIPortal` and `DocsPortal` supporting Scalar, Swagger UI, ReDoc, Stoplight Elements, and RapiDoc with AsyncAPI 3.0 and MCP discovery ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- WebMCP auto-registration and browser discovery via `registerWebMCPTools`, `WebMCPManifest`, and `/.well-known/webmcp.json` ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- Automated weekly dependency update configuration via Dependabot (`.github/dependabot.yml`) for GitHub Actions, Cargo, npm, and pip ecosystems ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- Automated GitHub Actions security workflow (`.github/workflows/security.yml`, `.github/zizmor.yml`) with Zizmor analysis and GitHub Advanced Security SARIF reporting ([PR #58](https://github.com/cofin/vertebra/pull/58)).

### Changed

- Decoupled `vertebra.extensions.litestar` from internal `vertebra.framework.*` modules, routing shared routing primitives through `vertebra.server._routing` (`RouteMatch`, `RouteEntry`, `compile_route_pattern`, `match_path`) ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- Removed redundant `framework` optional dependency extra from `pyproject.toml` now that `msgspec>=0.18` is a core runtime dependency ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- Modernized all GitHub Actions workflows (`.github/workflows/*.yml`) to latest major versions (`actions/checkout@v7`, `astral-sh/setup-uv@v7`, `dorny/paths-filter@v4`, `actions/upload-artifact@v7`, `actions/download-artifact@v8`, `softprops/action-gh-release@v3`, `pypa/cibuildwheel@v4.3.5`) on Node 24 runners with credential persistence hardening ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- Upgraded workspace dependencies via `make upgrade` (including `pytest-databases`) ([PR #58](https://github.com/cofin/vertebra/pull/58)).

### Fixed

- Handled dynamic code object syntax errors in `coverage xml` generation by enabling `ignore_errors = true` in `pyproject.toml` ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- Restored `@vertebra/client/router` export documentation parity in `docs/reference/typescript-client.md` ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- Fixed TypeScript `WebMCPManifest` vs `RegisterWebMCPOptions` options typing union resolution in `src/ts/vertebra-client/src/webmcp.ts` for DTS builds ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- Fixed wheel bundle sync fallback handling when local distribution wheels are present in debug environments ([PR #58](https://github.com/cofin/vertebra/pull/58)).

## 0.1.0

Initial release of Vertebra (`vertebra-core` on PyPI).

### Added

- Native Rust HTTP ingress and IPC runtime (`vertebra-core`, `vertebra-http`) supporting Hyper HTTP/1.1 and HTTP/2, ALPN TLS, optional HTTP/3 over QUIC, and gRPC wire framing.
- Zero-copy shared-memory IPC runtime supporting dual topologies (`Embedded` and `Shm`) with container-aware ring sizing and `/tmp/vtb-*` overflow fallback.
- Python application framework (`vertebra.framework`) supporting native handlers (`RequestBatch`, `Request`, `Response`, `Headers`), ASGI 3.0 (`AsgiRunner`), RSGI 1.6 (`RsgiAdapter`), and PEP 3333 WSGI (`WsgiAdapter`).
- Free-threaded Python 3.14t and 3.15t support across native PyO3 extensions (`vertebra._vertebra`, `vertebra._bundler`, `vertebra._storage`) with `gil_used = false` and PEP 810 lazy module exports.
- Native multicloud object storage (`vertebra.storage`, `_storage`) wrapping `object_store` with AWS S3, Google Cloud Storage, Azure Blob, OCI, and Cloudflare R2 presigning and transfer tracking.
- Shared-memory database and cache data plane (`vertebra.db`, `_vertebra`) across PostgreSQL, MySQL, SQLite, Oracle Thin TNS, SQL Server TDS, DuckDB, Redis, and Valkey RESP3.
- Full-stack TypeScript client SDK (`@vertebra/client`) with HTTP, WebSocket, SSE, HMR, and LiveState synchronization across React, Vue, Svelte, Angular, and HTMX adapters.
- Developer CLI toolchain (`vtb`) providing `run`, `dev`, `init`, `js`, `build`, and `mcp bridge` commands.
