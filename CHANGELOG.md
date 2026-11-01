# Changelog

All notable changes to Vertebra (`vertebra-core`) are documented in this file.

## 0.3.0

Release introducing shared-memory vector embeddings, zero-copy PEP 3118 vector views, binary database vector codecs, active Google Cloud ADC credential refresh, the `vtb ai` CLI, co-located background worker lanes, automatic A2A `AgentInterface` and dynamic request-origin URL resolution, updated isometric axial vertebrae branding, and expanded topic documentation.

### Added

- Shared-memory vector storage and SIMD kernels in `vertebra-core` (`VectorSlab` with `VTBVSLAB` header, set-associative seqlock `VectorCache`, `SimilarityMatch`, `StaticEmbedderCore`, Matryoshka Representation Learning `mrl_truncate_and_normalize_f32`, `quantize_f32_to_int8`, `quantize_f32_to_binary`, and SIMD cosine/dot/L2/Hamming `top_k_*` search) ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Zero-copy PEP 3118 buffer protocol types `VectorView` and `VectorBatchView` in `vertebra._vertebra` with `.truncate()`, `.quantize("int8" | "binary")`, `.top_k()`, `.to_pgvector_bytes()`, `.to_oracle_bytes()`, `.to_sqlite_blob()`, and `.to_arrow_shm()` ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Binary database vector codecs for PostgreSQL `pgvector` (`encode_pgvector_f32`, `decode_pgvector_f32`, `encode_pgvector_f16`, `decode_pgvector_f16`, `encode_pgvector_bit`, `decode_pgvector_bit`), Oracle 26ai `VECTOR` (`0xDB` binary wire format via `encode_oracle_vector_f32`, `decode_oracle_vector_f32`, `encode_oracle_vector_int8`, `encode_oracle_vector_binary`), and SQLAlchemy `Vector` type bind/result pushdown ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Native HTTP/2 gRPC 5-byte Length-Prefixed Message (LPM) packed-`f32` protobuf framing (`encode_gemini_embed_grpc_frame`, `decode_gemini_embed_grpc_frame`, `decode_gemini_embed_grpc_frame_to_batch`) in `vertebra-http` and `vertebra._vertebra` ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Google-first and local shared-memory embedding providers in `vertebra.embeddings`: `Embedder`, `VertexEmbedder`, `GeminiEmbedder`, `AlloyDbEmbedder`, `OracleEmbedder`, `RemoteEmbedder`, `create_embedding_grpc_service`, `GemmaEmbedder` (with `fp16` activation overflow guard), `FastEmbedder`, `StaticEmbedder`, `EmbeddingBroker` (single `1x` RAM model hosting across workers, token-budgeted length-sorted dynamic batching, and `3.14t`/`3.15t` subprocess GIL quarantine), and `ModelSpec` catalog ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Active Google Cloud Application Default Credentials (ADC) token cache and refresh provider (`GcpTokenSnapshot`, `GcpAuthSource`, `GcpTokenCache`, `RefreshingGcpCredentialProvider`) in `vertebra-storage` and `vertebra.storage` with `300s` proactive refresh skew, single-flight refresh deduplication, generation-checked `401` invalidation, and automatic DuckDB `CREATE OR REPLACE SECRET` token rotation in `SyncDuckDBDriver` and `AsyncDuckDBDriver` ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Consolidated `vtb ai` CLI group (`vertebra.cli.ai`) providing `vtb ai models list|pull|stage|warm` and `vtb ai mcp serve|bridge` ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Co-located background `WorkerRunner` execution in `vtb run` and `vtb dev` via `--with-workers`, `--queues`, `--lane-concurrency`, and `[tool.vertebra.workers]` in `pyproject.toml`, plus `--oneshot` and `--payload` in `vtb workers run` (`execute_worker_cli`) ([PR #64](https://github.com/cofin/vertebra/pull/64)).
- Automatic default `AgentInterface` synthesis (`protocol_binding="JSONRPC"`, `protocol_version="1.0"`) on `AgentCard` and dynamic request-origin URL resolution (`Forwarded`, `X-Forwarded-Proto`, `X-Forwarded-Host`, `Host` with `Vary` response headers) in `A2AHttpHandler` and `A2AService` ([PR #64](https://github.com/cofin/vertebra/pull/64)).

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
