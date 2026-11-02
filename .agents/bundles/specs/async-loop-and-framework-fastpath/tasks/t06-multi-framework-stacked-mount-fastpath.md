---
type: Task
id: async-loop-and-framework-fastpath:t06
title: "Auto-attach stacked FastPath dispatchers across App.mount() and share native database pools & SHM LeaseBroker across all 6 frameworks"
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - async-loop-and-framework-fastpath:t05
files:
  - src/py/vertebra/server/_fastpath.py
  - src/py/vertebra/server/worker.py
  - src/py/vertebra/server/_wsgi/_adapter.py
  - src/py/vertebra/db/_registry.py
  - src/py/vertebra/extensions/django/db/backends/__init__.py
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
tests:
  - src/py/tests/unit/extensions/test_fastapi.py
  - src/py/tests/unit/extensions/test_django.py
  - src/py/tests/unit/server/test_asgi_capi_runner.py
plan_revision: 2
plan_commit: 34429ff8c50d4622d0cacbd1288525f8a1211dc8
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: 2026-11-01T23:48:00Z
updated_at: 2026-11-02T00:03:00Z
commit: null
tags: [server, fastpath, mount, database, multi-framework, litestar, fastapi, starlette, flask, django, tdd]
---

# Task t06: Auto-attach stacked `FastPath` dispatchers across `App.mount()` and share native database pools & SHM `LeaseBroker` across all 6 frameworks

## Objective
1. Implement `attach_framework_fastpath` in `src/py/vertebra/server/_fastpath.py` and wire it into `src/py/vertebra/server/worker.py::_instantiate_mount_runners` and `src/py/vertebra/server/_wsgi/_adapter.py::WsgiAdapter` so stacking or combining any combination of `Vertebra Native`, `Litestar`, `FastAPI`, `Starlette`, `Flask`, and `Django` (standalone or mounted via `app.mount("/prefix", sub_app)`) automatically compiles and executes each framework's `FastPath` with prefix-aware route matching.
2. Add canonical endpoint fingerprinting (`DatabaseRegistry.pool_fingerprint`) and process-wide shared pool/broker resolution (`DatabaseRegistry.default()`, `DatabaseRegistry.default_broker()`, `get_shared_driver`, `get_shared_sync_driver`) in `src/py/vertebra/db/_registry.py`, and wire `share_pool: bool = True` across `DatabaseWrapperMixin` (`src/py/vertebra/extensions/django/db/backends/__init__.py`), `SyncAdapterConfig` / `AsyncAdapterConfig` (`src/py/vertebra/extensions/sqlspec/adapters.py`), and `DBAPIConnection` / `AsyncDBAPIConnection` (`src/py/vertebra/extensions/sqlalchemy/dialects.py`) so native database adapters declared independently across stacked apps automatically reuse the same underlying connection pool, `CloudSQLDialer` / `AlloyDBDialer`, SHM `LeaseBroker`, and `ShmStatementCache`.

## Context
- **Verification Strategy (`behavior_tdd`)**: Write failing tests in `src/py/tests/unit/server/test_asgi_capi_runner.py`, `src/py/tests/unit/extensions/test_fastapi.py`, and `src/py/tests/unit/extensions/test_django.py` verifying:
  - Mounting `Litestar`, `FastAPI`, `Starlette`, `Flask`, and `Django` sub-applications onto a Vertebra `App` via `app.mount(...)` auto-attaches each framework's `FastPath` (idempotently when already wrapped) and dispatches mounted requests through `drive_asgi_batch` / `drive_wsgi_batch` with `root_path` prefix stripping.
  - Declaring a native database adapter in a `Litestar` app (`PostgreSQLSyncConfig` / `SQLiteSyncConfig` or `postgresql+vtb://` / `sqlite+vtb://` SQLAlchemy dialect) and a `Django` app (`vertebra.extensions.django.db.backends.postgresql` / `sqlite3`) targeting the same database endpoint reuses the exact same `SyncPostgreSQLDriver` / `SyncSQLiteDriver` instance, `CloudSQLDialer` / `AlloyDBDialer`, and process-wide `LeaseBroker`.
- **Architectural Invariants**:
  - `attach_framework_fastpath(app, *, mount_prefix="", protocol="auto")` must be idempotent: if `getattr(app, "__vertebra_fastpath_attached__", False)` is already `True`, do not double-wrap.
  - Detect framework apps by class/module inspection (`litestar`, `fastapi`, `starlette`, `flask`, `django`) or attached `_vertebra_fastpath` / `RouteCompiler` attributes without eagerly importing uninstalled optional frameworks.
  - `DatabaseRegistry.default()` must allow opting out of pool sharing when `share_pool=False` is passed explicitly in options/config.

## Steps
1. **Write failing unit tests (`RED`)**:
   - In `src/py/tests/unit/server/test_asgi_capi_runner.py` and `src/py/tests/unit/extensions/test_fastapi.py`, add tests that:
     - Mount `Litestar`, `FastAPI`, `Starlette`, and `Flask` sub-apps onto a Vertebra `App` (`app.mount("/litestar", litestar_app)`, `app.mount("/fastapi", fastapi_app)`, `app.mount("/starlette", starlette_app)`, `app.mount("/flask", flask_app)`).
     - Build mount runners via `_instantiate_mount_runners(app.mounts)` and verify that each mounted runner has `__vertebra_fastpath_attached__ is True` and executes prefixed requests (`/litestar/items/42`, `/fastapi/items/42`, `/starlette/items/42`, `/flask/items/42`) via C-API `drive_asgi_batch` / `drive_wsgi_batch` with `send_raw_response`.
   - In `src/py/tests/unit/extensions/test_django.py`, add `test_cross_framework_shared_native_pool_and_lease_broker` verifying that a Django `DatabaseWrapper` (`vertebra.extensions.django.db.backends.postgresql` and `sqlite3`), a SQLSpec native `PostgreSQLSyncConfig` / `SQLiteSyncConfig`, and a SQLAlchemy `postgresql+vtb://` / `sqlite+vtb://` `DBAPIConnection` pointing to the same database endpoint all resolve the identical shared native driver instance (`wrapper.native_driver is sqlspec_driver is sa_conn.driver`) and share `DatabaseRegistry.default_broker()`.
2. **Implement `attach_framework_fastpath` and cross-framework shared native database pools (`GREEN`)**:
   - In `src/py/vertebra/db/_registry.py`:
     - Add class-level singleton `_DEFAULT_REGISTRY: DatabaseRegistry | None = None` and `_DEFAULT_BROKER: LeaseBroker | None = None` with `@classmethod def default(cls) -> DatabaseRegistry` and `@classmethod def default_broker(cls, *, pool_size: int = 32, max_overflow: int = 16) -> LeaseBroker` (checking `LeaseBroker.from_env()` first before creating a process-wide `LeaseBroker`).
     - Add `@classmethod def pool_fingerprint(cls, config_or_url: AnyDatabaseConfig | str) -> tuple[Any, ...]` normalizing scheme/vendor, host/path, port, database, user, pooler_mode, `cloud_sql_instance`, and `alloydb_instance_uri`.
     - Add `get_shared_driver(self, config_or_url: AnyDatabaseConfig | str) -> AnyDatabaseDriver` (sharded per `(fingerprint, id(running_loop))` when an event loop is running) and `get_shared_sync_driver(self, config_or_url: AnyDatabaseConfig | str) -> AnySyncDatabaseDriver` (keyed by `fingerprint` with shared `default_broker()` and shared `CloudSQLDialer` / `AlloyDBDialer`).
   - In `src/py/vertebra/extensions/django/db/backends/__init__.py::DatabaseWrapperMixin`:
     - Support `share_pool: bool = True` (via `OPTIONS.get("share_pool", True)`), attach `self._broker = DatabaseRegistry.default_broker(pool_size=self.pooler_config.max_connections)` when `share_pool` is `True`, and expose `self.native_driver` / `self.async_native_driver` resolved from `DatabaseRegistry.default()`.
   - In `src/py/vertebra/extensions/sqlspec/adapters.py` (`SyncAdapterConfig` / `AsyncAdapterConfig`) and `src/py/vertebra/extensions/sqlalchemy/dialects.py` (`DBAPIConnection` / `AsyncDBAPIConnection`):
     - Resolve native drivers through `DatabaseRegistry.default().get_shared_sync_driver(config)` / `get_shared_driver(config)` by default (`share_pool: bool = True`) so SQLSpec, SQLAlchemy `+vtb`, and Django share the same pool and `LeaseBroker`.
   - In `src/py/vertebra/server/_fastpath.py`:
     - Implement `attach_framework_fastpath(app: Any, *, mount_prefix: str = "", protocol: str = "auto") -> Any` detecting:
       - ASGI apps with an existing `FastPath` (`getattr(app, "_vertebra_fastpath", None)`), or raw `Litestar` (`LitestarFastPathBypass.from_app(app).wrap_asgi(app)`), `FastAPI` (`FastAPIFastPath.from_app(app).wrap_asgi(app)`), or `Starlette` (`StarletteFastPath.from_app(app).wrap_asgi(app)`) instances, marking `__vertebra_fastpath_attached__ = True`.
       - `Flask` apps: compiling `RouteCompiler(app)` + `FlaskFastPath(compiler)` and wrapping WSGI `app.wsgi_app` via `fastpath.wrap_wsgi(app.wsgi_app)` (or `fastpath.wrap_asgi()` when `protocol == "asgi"`).
       - `Django` WSGI/ASGI handlers or `DjangoFastPath` instances: wrapping via `fastpath.wrap_wsgi(app)` or `fastpath.wrap_asgi(app)`.
   - In `src/py/vertebra/server/worker.py::_instantiate_mount_runners` and `src/py/vertebra/server/_wsgi/_adapter.py::WsgiAdapter`:
     - Call `app = attach_framework_fastpath(app, mount_prefix=prefix, protocol=resolved)` before constructing `AsgiRunner`, `_AsgiMountRunner`, or `WsgiAdapter`.
     - Ensure `WsgiAdapter` strips `self._mount_prefix` from `PATH_INFO` when calling WSGI `FastPath` or `drive_wsgi_batch`.
3. **Refactor & verify (`REFACTOR`)**:
   - Run `make lint` and the full unit test suite across server and framework extensions.

## Verification
```bash
make lint
uv run --no-sync pytest src/py/tests/unit/server/test_asgi_capi_runner.py src/py/tests/unit/server/test_wsgi_batch_runner.py src/py/tests/unit/server/test_litestar_direct_fastpath.py src/py/tests/unit/extensions/test_fastapi.py src/py/tests/unit/extensions/test_starlette.py src/py/tests/unit/extensions/test_flask.py src/py/tests/unit/extensions/test_django.py -q
```

## Acceptance Criteria
- [ ] `attach_framework_fastpath` auto-detects and idempotently wraps `Litestar`, `FastAPI`, `Starlette`, `Flask`, and `Django` apps with their zero-copy `FastPath` (R6).
- [ ] `_instantiate_mount_runners` attaches `FastPath` to mounted sub-applications and strips `mount_prefix` so stacked multi-framework apps execute at C-API fastpath speed (R6).
- [ ] `DatabaseRegistry.default()` shares the same underlying native driver pool, `CloudSQLDialer` / `AlloyDBDialer`, and process-wide SHM `LeaseBroker` across `Django`, `SQLSpec`, `SQLAlchemy (+vtb)`, and `Vertebra Native` when targeting the same database endpoint (R7).

## Notes & Discoveries
