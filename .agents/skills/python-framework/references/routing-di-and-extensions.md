# Routing, Dependency Injection, and Framework Extensions

## Route & Procedure Compilation (`vertebra/framework/_app.py`, `vertebra/framework/_router.py`, `vertebra/server/_dispatch.py`)

- **Declarative Routing**: `App`, `Router`, and `Controller` register HTTP routes (`@get`, `@post`, `@put`, `@patch`, `@delete`, `Route`) and typed RPC procedures (`@query`, `@mutation`, `@subscription`, `Procedure`, `ProcedureRouter`).
- **Startup Compilation**: At startup, routes compile into deterministic FNV-1a 64-bit handler IDs (`handler_id(method, path)`) synchronized with the Rust radix router (`vertebra-http::routing::RouteTable`), and `compile_handler_spec` pre-builds path/query/header/cookie extractors, `msgspec` decoders (`Request.decode_msgspec`), guards, and sync/async DI resolvers.

## Hierarchical Dependency Injection (`vertebra/framework/di.py`)

- Primitives: `Provide`, `Inject`, `Container`, `DependencyGraph`, and `RequestScope`.
- Scopes: `"singleton"` (cached per application lifecycle), `"request"` (cached per HTTP/RPC request context with generator `yield` cleanup), and `"transient"` (instantiated per injection site).
- Dependencies can be declared at the `App`, `Router`, `Controller`, or individual `Route`/`Procedure` level; inner scopes override outer scopes cleanly.

## Framework Extensions (`vertebra/extensions/`)

- **Litestar (`vertebra.extensions.litestar`)**: `VertebraPlugin` (`VertebraConfig`) compiles Litestar v2/v3 routes into the Rust router, overrides `litestar run` via `on_cli_init(cli)`, offloads CORS/compression/static assets, activates `FastPath`, and remains decoupled from `vertebra.framework.*` with all Litestar ecosystem plugins (`queues`, `security`, `autowire`, `email`, `mcp`, `vite`, `htmx`, `saq`, `asyncapi`) strictly optional.
- **Starlette & FastAPI (`vertebra.extensions.starlette`, `vertebra.extensions.fastapi`)**: `VertebraPlugin` (`VertebraConfig`), ASGI lifecycle integration, DAG `DependencyPlan`, typed state injection, and Rust ingress mounting.
- **Flask & Django (`vertebra.extensions.flask`, `vertebra.extensions.django`)**: `VertebraPlugin` (`VertebraConfig`, `VertebraAppConfig`), zero-copy PEP 3333 WSGI batch bridge (`drive_wsgi_batch`), and 6 native Django database engines (`vertebra.extensions.django.db.backends.{postgresql,mysql,sqlite3,oracle,mssql,duckdb}`) with `DatabaseWrapperMixin` and `ArrowQuerySetMixin`.
- **SQLSpec, Advanced Alchemy & SQLAlchemy (`vertebra.extensions.{sqlspec,alchemy,sqlalchemy}`)**: Native Rust SHM database adapters (`{Postgres,Mysql,Sqlite,Oracle,Mssql,Duckdb}{Sync,Async}Config`), universal SHM broker proxies (`ProxySyncConfig`, `ProxyAsyncConfig`), `+vtb` dialects, `ObjectStorageBackend`, and `SQLAlchemyCacheBackend`.
