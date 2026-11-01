# Changelog

All notable changes to Vertebra are documented on this page.

## 0.3.0

Release introducing shared-memory vector embeddings, zero-copy PEP 3118 vector views, binary database vector codecs, active Google Cloud ADC credential refresh, the `vtb ai` CLI, co-located background worker lanes, automatic A2A `AgentInterface` and dynamic request-origin URL resolution, updated isometric axial vertebrae branding, and expanded topic documentation.

### Added

- **Shared-Memory Vector Primitives & PEP 3118 Views (`vertebra-core`, `vertebra._vertebra`)**:
  - Cache-line-aligned POSIX shared-memory `VectorSlab` (`VTBVSLAB` header), set-associative seqlock `VectorCache`, `SimilarityMatch`, `StaticEmbedderCore`, Matryoshka `mrl_truncate_and_normalize_f32`, `quantize_f32_to_int8`, `quantize_f32_to_binary`, and SIMD `top_k_*` kernels ([PR #64](https://github.com/cofin/vertebra/pull/64)).
  - Zero-copy PEP 3118 `VectorView` and `VectorBatchView` with `.truncate()`, `.quantize()`, `.top_k()`, `.to_pgvector_bytes()`, `.to_oracle_bytes()`, `.to_sqlite_blob()`, and `.to_arrow_shm()` ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- **Binary Database Vector Codecs (`vertebra-db`, `vertebra.extensions.sqlalchemy`)**:
  - PostgreSQL `pgvector` binary wire codecs (`encode_pgvector_f32`, `decode_pgvector_f32`, `encode_pgvector_f16`, `decode_pgvector_f16`, `encode_pgvector_bit`, `decode_pgvector_bit`), Oracle 26ai `0xDB` binary `VECTOR` codecs (`encode_oracle_vector_f32`, `decode_oracle_vector_f32`, `encode_oracle_vector_int8`, `encode_oracle_vector_binary`), and SQLAlchemy `Vector` bind/result pushdown ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- **Google-First & Local Shared-Memory Embedders (`vertebra.embeddings`, `vertebra-http`)**:
  - Native HTTP/2 gRPC packed-`f32` framing (`encode_gemini_embed_grpc_frame`, `decode_gemini_embed_grpc_frame_to_batch`) and `Embedder`, `VertexEmbedder`, `GeminiEmbedder`, `AlloyDbEmbedder`, `OracleEmbedder`, `RemoteEmbedder`, and `create_embedding_grpc_service` ([PR #64](https://github.com/cofin/vertebra/pull/64)).
  - `EmbeddingBroker` (`1x` RAM model hosting across workers, token-budgeted length-sorted dynamic batching, and `3.14t`/`3.15t` subprocess GIL quarantine), `GemmaEmbedder`, `FastEmbedder`, `StaticEmbedder`, and `ModelSpec` catalog ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- **Active GCP ADC Credential Refresh & Non-Blocking Storage (`vertebra-storage`, `vertebra.storage`, `vertebra.db.duckdb`)**:
  - `GcpTokenSnapshot`, `GcpAuthSource`, `GcpTokenCache`, and `RefreshingGcpCredentialProvider` with `300s` proactive refresh skew, single-flight refresh deduplication, generation-checked `401` invalidation, and automatic DuckDB `CREATE OR REPLACE SECRET` token rotation in `SyncDuckDBDriver` and `AsyncDuckDBDriver` ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- **`vtb ai` CLI, Co-Located Workers & Dynamic A2A Discovery (`vertebra.cli`, `vertebra.agent.a2a`)**:
  - `vtb ai models list|pull|stage|warm` and `vtb ai mcp serve|bridge` in `vertebra.cli.ai` ([PR #64](https://github.com/cofin/vertebra/pull/64)).
  - Co-located `WorkerRunner` execution in `vtb run` and `vtb dev` (`--with-workers`, `--queues`, `--lane-concurrency`, `[tool.vertebra.workers]`) and `--oneshot` / `--payload` in `vtb workers run` ([PR #64](https://github.com/cofin/vertebra/pull/64)).
  - Automatic default `AgentInterface` synthesis on `AgentCard` and dynamic request-origin URL resolution (`Forwarded`, `X-Forwarded-Proto`, `X-Forwarded-Host`, `Host` with `Vary` headers) in `A2AHttpHandler` and `A2AService` ([PR #64](https://github.com/cofin/vertebra/pull/64)).

### Changed

- Replaced blocking `runtime().block_on(...)` calls in `vertebra-storage` async PyO3 operations with non-blocking Tokio `spawn_rust_task` execution (`TaskCompletionCallback`) ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Replaced top-level `vtb mcp` CLI command group with `vtb ai mcp` (`serve` and `bridge`) ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Updated brand logo and icon assets (`docs/assets/`) to an isometric axial vertebrae mark (`#EDB641`), pinned persistent `VERTEBRA / <Current Page>` header context in `docs/stylesheets/extra.css`, and split usage documentation into 14 focused topic guides under `docs/usage/` ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Updated `.github/dependabot.yml` to use native `bun` and `uv` package ecosystems with grouped minor/patch updates and `open-pull-requests-limit: 1`, gated CI test matrices on `lint` with `fail-fast: true` in `.github/workflows/ci.yml`, and upgraded workspace dependencies via `make upgrade` (`anyio`, `bump-my-version`, `platformdirs`, `pytest-databases`, `python-dateutil`, `sqlparse`, `crc32fast`) ([PR #64](https://github.com/cofin/vertebra/pull/64)).

### Fixed

- Fixed `dorny/paths-filter@v4` shallow-clone `git fetch` failure on `push` events to `main` by setting `fetch-depth: 2` in `.github/workflows/ci.yml`, and removed failing `git push` commit step from `.github/workflows/benchmark.yml` ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Scoped `[tool.bumpversion.files]` search/replace in `pyproject.toml` to `version = "{current_version}"` so `make release` does not rewrite dependency version floors, and automated `uv.lock` and `Cargo.lock` synchronization in `Makefile` `release`/`pre-release` targets ([PR #64](https://github.com/cofin/vertebra/pull/64)).

## 0.2.0

Comprehensive release introducing zero-copy serialization bridges, real-time database channels, Advanced Alchemy object storage and cache services, bidirectional Protobuf/gRPC tooling, multi-framework extensions, OpenAPI documentation portals, WebMCP auto-registration, and modernized repository security tooling.

### Added

- **Zero-Copy C-API Serialization & Row Hydration (`_zero_copy.py`, `vertebra.server`)**:
  - Direct C-API decoding into `msgspec.Struct` without intermediate Python allocations via `Request.decode_msgspec`, `WsgiInput.decode_msgspec`, and `AsgiScope.decode_msgspec` ([PR #58](https://github.com/cofin/vertebra/pull/58)).
  - Vectorized row hydration via `hydrate_rows_vectorcall` and thread-local scratch arenas in `_zero_copy.py` for high-throughput batch query results ([PR #58](https://github.com/cofin/vertebra/pull/58)).
  - Direct struct-to-wire response serialization via `encode_msgspec_response` ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- **Database Channels & Multi-Node Synchronization (`vertebra.db`, `vertebra.server`)**:
  - Database-backed multi-node channel transport via `DatabaseChannelDriver`, `DatabaseChannelConfig`, `DatabaseChannelSubscription`, and `DatabaseChannelPublisher` supporting PostgreSQL `LISTEN`/`NOTIFY`, MySQL advisory locks, SQLite WAL triggers, and message queuing ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- **Advanced Alchemy Storage & Cache Integrations (`vertebra.extensions.advanced_alchemy`)**:
  - Object storage repository service bridge via `ObjectStorageBackend`, `ObjectStorageConfig`, and `ObjectStorageFileService` ([PR #58](https://github.com/cofin/vertebra/pull/58)).
  - Async cache manager for repository query caching via `AsyncCacheManager` and `AsyncCacheConfig` ([PR #58](https://github.com/cofin/vertebra/pull/58)).
  - Zero-copy service mixin via `ZeroCopyServiceMixin` providing vectorized ORM-to-wire DTO transformations ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- **Bidirectional Protobuf/gRPC Tooling & Binary Framing (`vertebra.server._proto`, `_lpm`)**:
  - Protobuf parser and code generation via `ProtobufParser`, `ProtobufPythonGenerator`, `ProtobufTypeScriptGenerator`, and route-to-proto inspection via `ProtobufGenerator.from_routes` ([PR #58](https://github.com/cofin/vertebra/pull/58)).
  - Length-prefixed message (LPM) binary framing (`encode_lpm_frame`, `decode_lpm_frame`) for gRPC and streaming RPC transports ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- **Multi-Framework Extensions (`vertebra.extensions`)**:
  - Native Vertebra server integration and lifecycle management across Starlette (`StarletteExtension`), FastAPI (`FastAPIExtension`), Flask (`FlaskExtension`), and Django (`DjangoExtension`) ([PR #58](https://github.com/cofin/vertebra/pull/58)).
  - Six native Django database backends with Arrow acceleration via `DatabaseWrapperMixin`, `ArrowQuerySetMixin`, `VertebraPostgresBackend`, `VertebraMysqlBackend`, `VertebraSqliteBackend`, `VertebraOracleBackend`, `VertebraMssqlBackend`, and `VertebraDuckdbBackend` ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- **OpenAPI & Interactive Documentation Portals (`vertebra.server._openapi`, `_docs_portal`)**:
  - Unified interactive API documentation via `OpenAPIPortal` and `DocsPortal` supporting Scalar, Swagger UI, ReDoc, Stoplight Elements, and RapiDoc ([PR #58](https://github.com/cofin/vertebra/pull/58)).
  - AsyncAPI 3.0 specification generation and Model Context Protocol (MCP) server manifest discovery endpoints (`/.well-known/mcp.json`) ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- **WebMCP Auto-Registration & Browser Discovery (`@vertebra/client/webmcp`, `vertebra.server._webmcp`)**:
  - Client and server WebMCP discovery via `registerWebMCPTools`, `WebMCPManifest`, and `/.well-known/webmcp.json` ([PR #58](https://github.com/cofin/vertebra/pull/58)).
- **CI Modernization, Dependabot & Security Automation**:
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

Initial release of Vertebra (`vertebra-core` on PyPI and `@vertebra/client` on npm).

### Added

- **Rust HTTP Ingress & Shared-Memory IPC (`vertebra-core`, `vertebra-http`)**:
  - Hyper HTTP/1.1 and HTTP/2 server, ALPN TLS, optional HTTP/3 over QUIC, gRPC wire framing, WebSocket, and Server-Sent Events (SSE).
  - Lock-free SPSC and MPMC shared-memory ring buffers (`ShmHeader`, `SpscRing`, `MpmcRing`, `TieredRingSet`) and embedded `BatchBridge` with adaptive `futex`/`ulock` and `eventfd` signaling.
  - Opt-in `BackpressureController` with bounded in-flight permits, queue timeouts, and sliding-window circuit breaking.
- **Python Application Framework & Multi-Protocol Runners (`vertebra.framework`, `vertebra.server`)**:
  - `App`, `Controller`, `Router`, `Route`, `Procedure`, `ProcedureRouter`, `Container`/`DependencyGraph`/`Provide`/`Inject` dependency injection, `Guard`/`SecurityConfig`, `OpenAPIConfig`, `LiveState`, and `QueueService`/`CronScheduler`.
  - Multi-protocol worker execution supporting native `RequestBatch` handlers, ASGI 3.0 (`AsgiRunner`), RSGI 1.6 (`RsgiAdapter`), and PEP 3333 WSGI (`WsgiAdapter`) across isolated worker lanes (`fast`, `async`, `cpu`, `blocking`).
- **Free-Threaded Python Support (`3.14t` / `3.15t`)**:
  - Native PyO3 extensions (`vertebra._vertebra`, `vertebra._bundler`, `vertebra._storage`) compiled with `gil_used = false` and PEP 561 `.pyi` type stubs.
- **Frontend Bundler, QuickJS SSR & TypeScript SDK (`vertebra-bundler`, `@vertebra/client`)**:
  - Embedded Rolldown/Oxc bundler (`Bundler`, `BundleConfig`) and pooled QuickJS server-side rendering engine (`EmbeddedSsrClient`, `SsrConfig`) with Inertia v3 and HTMX integration.
  - `@vertebra/client` TypeScript SDK providing `HttpClient`, `SseClient`, `WebSocketClient`, `LiveStateClient`, `ApexRegion`, `WasmClient`, `ArrowStreamClient`, `McpClient`, `A2aClient`, and framework adapters for React, Vue, Svelte, Angular, HTMX, and Vite.
- **Native Storage, Database Drivers & Telemetry (`vertebra.storage`, `vertebra.db`, `vertebra.telemetry`)**:
  - Multicloud and local object storage (`ObjectStore`, `SyncStore`, `AsyncStore`) backed by Rust `object_store` (S3, GCS with ADC, Azure, R2, OCI, HTTP, Local, Memory) with presigned URLs and PyArrow range readers.
  - Native database drivers (`PostgresConfig`, `MysqlConfig`, `SqliteConfig`, `SyncSqliteDriver`, `AsyncSqliteDriver`) with OpenTelemetry `sqlcommenter` trace propagation.
- **Agent Protocols (`vertebra.agent`)**:
  - Model Context Protocol (`MCPRegistry`, `MCPConfig`, `MCPSkillsConfig`, `MCPTaskConfig`, `run_stdio_http_bridge`) and Agent-to-Agent (`A2AService`, `A2AConfig`, `AgentCard`) support plus `AgentRuntime` and `WorkflowEngine`.
- **CLI & Packaging (`vertebra.cli`, `vertebra.build`)**:
  - `vtb` CLI (`run`, `dev`, `init`, `build`, `js`, `mcp`, `workers`) and standalone/single-file binary builders (`StandaloneBuilder`, `OnefileBuilder`, `BuildOrchestrator`).
