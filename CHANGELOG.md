# Changelog

All notable changes to Vertebra (`vertebra-core`) are documented in this file.

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
