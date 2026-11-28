---
type: Gotcha
title: Framework Extension Limitations
description: Operational boundaries across Django driver-free DatabaseWrapper backends, Flask-Caching extra requirements, Starlette/FastAPI init_app idempotency, ShmQueueRing macOS 31-byte POSIX SHM hashing, and optional host import gates.
status: stable
tags:
  - framework
  - extensions
  - django
  - flask
  - starlette
  - fastapi
  - shm
  - limitations
generated:
  by: flow/0.30.1
  at: "2026-11-17T05:50:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-17T05:50:00Z"
    commit: 5a833832
sources:
  - id: django-mysql-base
    resource: src/py/vertebra/extensions/django/db/backends/mysql/base.py
    title: Driver-free Django MySQL BaseDatabaseWrapper subclass
  - id: django-oracle-base
    resource: src/py/vertebra/extensions/django/db/backends/oracle/base.py
    title: Driver-free Django Oracle BaseDatabaseWrapper subclass
  - id: django-spanner-base
    resource: src/py/vertebra/extensions/django/db/backends/spanner/base.py
    title: Django Spanner BaseDatabaseWrapper requiring django-google-spanner
  - id: flask-cache-py
    resource: src/py/vertebra/extensions/flask/cache.py
    title: Lazy FlaskCachingBackend subclassing flask_caching BaseCache
  - id: starlette-plugin-py
    resource: src/py/vertebra/extensions/starlette/plugin.py
    title: Starlette VertebraPlugin.init_app idempotency guard
  - id: fastapi-plugin-py
    resource: src/py/vertebra/extensions/fastapi/plugin.py
    title: FastAPI VertebraPlugin.init_app idempotency guard
  - id: queue-ring-rs
    resource: src/rs/vertebra-py/src/queue_ring.rs
    title: ShmQueueRing POSIX shared-memory path hashing and bounds
  - id: pyproject-toml
    resource: pyproject.toml
    title: Optional host extra dependencies for framework extensions
  - id: litestar-vite-plugin-py
    resource: src/py/vertebra/extensions/litestar/vite/plugin.py
    title: Slotted VertebraVitePlugin(VitePlugin) exposing client_aliases
---

# Framework Extension Limitations

<!-- truth: start -->
- Django `mysql`, `oracle`, and `spanner` `DatabaseWrapper` subclasses bind `vertebra.db.dbapi` without injecting `sys.modules` driver shims (`MySQLdb`, `oracledb`, `psycopg`); `spanner` still requires `django-google-spanner` (`django_spanner.base`) via `ensure_module`, and live host database conformance requires external database or emulator instances [^django-mysql-base] [^django-oracle-base] [^django-spanner-base].
- `vertebra.extensions.flask.cache.FlaskCachingBackend` subclasses `flask_caching.backends.base.BaseCache` lazily on attribute lookup and raises `MissingDependencyError` unless `flask-caching>=2.3.0` (the `flask` extra) is installed [^flask-cache-py] [^pyproject-toml].
- `VertebraPlugin.init_app(app)` in Starlette and FastAPI tracks `app.state._vertebra_initialized` to avoid stacking duplicate lifespan wrappers or ASGI middleware; repeated calls in Starlette return early before re-mounting routes, while FastAPI skips duplicate wrapping and re-runs `ensure_finalized(app)` [^starlette-plugin-py] [^fastapi-plugin-py].
- `ShmQueueRing` hashes logical ring names into 22-byte `/vtbq_<16-hex>` paths to stay under macOS's 31-byte POSIX `shm_open` limit (`#[cfg(unix)]`), enforces fixed `capacity` (`4096`), `slot_bytes` (`8192`), and `MAX_RECORD_KEY_BYTES` (`256`) bounds without disk spill, and relies on 64-bit hash uniqueness across concurrent ring names [^queue-ring-rs].
- Framework extensions (`litestar`, `starlette`, `fastapi`, `flask`, `django`) keep host imports under `TYPE_CHECKING` and load them lazily via `ensure_module`, raising `MissingDependencyError` when the target host package is absent [^pyproject-toml].
- `vertebra.extensions.litestar.vite.plugin.create_vite_plugin` binds a slotted `VertebraVitePlugin(VitePlugin)` subclass lazily via `ensure_module("litestar_vite", extra="vite")` to expose `client_aliases: dict[str, str]`; `tools/scripts/review_setup.py` `PY-07` (`allowed_vertebra_classes`) flags the `VertebraVitePlugin` `TYPE_CHECKING` declaration until the review manifest allowlist or `t12` updates it [^litestar-vite-plugin-py].
- `src/py/tests/unit/framework/queue_test_doubles.py` is `1,290` lines (`> 1,000`) because it consolidates both the original queue executor/stream doubles and the RESP store doubles (`InMemoryRESPQueueStoreDriver`, `InMemoryAsyncRESPQueueStoreDriver`) added ahead of the `test_distributed_queues.py` split.
- `pyproject.toml` pins `pyright==1.1.381` in `[dependency-groups].lint` and enables `[tool.pyright]` (`typeCheckingMode = "standard"`, `reportUnusedImport = "error"`, `reportUnusedVariable = "warning"`), while offline environments without PyPI access cannot refresh `uv.lock` or sync `.venv` and must verify against global `pyright` (`1.1.408`); unowned `src/py/vertebra/agent/mcp/client.py`, `src/py/vertebra/framework/email/providers.py`, `src/py/vertebra/server/_webtransport.py`, `agent/__init__.py`, `extensions/litestar/__init__.py`, and `extensions/{django,flask}/fastpath.py` still emit Pyright errors or warnings [^pyproject-toml].
<!-- truth: end -->

## Operational Boundaries

- **Driver-Free Django Database Backends**: [`vertebra.extensions.django.db.backends.mysql`](../../../../../src/py/vertebra/extensions/django/db/backends/mysql/base.py)[^django-mysql-base] and [`oracle`](../../../../../src/py/vertebra/extensions/django/db/backends/oracle/base.py)[^django-oracle-base] subclass Django's `BaseDatabaseWrapper` directly with `_MySQLIntrospection`, `_OracleIntrospection`, and `MySQLCursorWrapper` (`%s` to `?` placeholder translation) so Django does not import `MySQLdb` or `oracledb`. [`spanner`](../../../../../src/py/vertebra/extensions/django/db/backends/spanner/base.py)[^django-spanner-base] requires `django_spanner.base` (`django-google-spanner`) in `__init__` with no fallback base, and `postgresql` / `sqlite3` subclass Django's built-in wrappers. Unit suites verify `get_connection` and wrapper behavior without live external database servers.
- **`flask-caching` Extra Requirement**: [`FlaskCachingBackend`](../../../../../src/py/vertebra/extensions/flask/cache.py)[^flask-cache-py] binds `flask_caching.backends.base.BaseCache` inside `__getattr__`. Installing `flask` alone without `flask-caching>=2.3.0` (`vertebra-core[flask]` in [`pyproject.toml`](../../../../../pyproject.toml)[^pyproject-toml]) allows importing `vertebra.extensions.flask.cache.CacheBackend`, but resolving `FlaskCachingBackend` raises `MissingDependencyError`.
- **`init_app` Idempotency Scope**: In Starlette [`VertebraPlugin.init_app`](../../../../../src/py/vertebra/extensions/starlette/plugin.py)[^starlette-plugin-py], an already-initialized app returns immediately when `app.state._vertebra_initialized` is truthy. In FastAPI [`VertebraPlugin.init_app`](../../../../../src/py/vertebra/extensions/fastapi/plugin.py)[^fastapi-plugin-py], lifespan wrapping and `_install_asgi_middleware` run only once while `default_response_class` and `ensure_finalized(app)` re-evaluate on subsequent calls.
- **`ShmQueueRing` Platform & Capacity Limits**: [`queue_ring.rs`](../../../../../src/rs/vertebra-py/src/queue_ring.rs)[^queue-ring-rs] targets Unix `shm_open` (`#[cfg(unix)]`) and maps any ring name to `/vtbq_<digest:016x>` via `stable_hash64`. Names are bounded below macOS's 31-byte limit, record keys are capped at `256` bytes, payloads exceeding `slot_bytes` or a full ring raise Python errors instead of spilling, and hash collisions between unrelated logical names would alias the same segment.
- **Optional Host Import Gates**: Every framework extension package isolates host imports behind `TYPE_CHECKING` and `ensure_module` without importing sibling framework extensions or `vertebra.extensions.alchemy`; calling an adapter without its host wheel installed fails with `MissingDependencyError`.
- **`VertebraVitePlugin` Slotted Subclass & `PY-07` Allowlist**: [`create_vite_plugin`](../../../../../src/py/vertebra/extensions/litestar/vite/plugin.py)[^litestar-vite-plugin-py] subclasses `litestar_vite.VitePlugin` dynamically with `__slots__ = ("client_aliases",)` so `isinstance(plugin, VitePlugin)` stays `True` without modifying `litestar_vite.ViteConfig`. Its `TYPE_CHECKING` stub `VertebraVitePlugin` is currently flagged by `tools/scripts/review_setup.py` `PY-07` because `allowed_vertebra_classes` omits `VertebraVitePlugin`.

Current framework extension capabilities live in [Framework Extensions: Litestar, Starlette, FastAPI, Flask, and Django](../extensions/litestar.md) and [Distributed Queues, Cron Leader Election & Transactional Email](distributed-queues-and-email.md).

[^django-mysql-base]: `src/py/vertebra/extensions/django/db/backends/mysql/base.py` — Driver-free Django MySQL `BaseDatabaseWrapper` subclass.
[^django-oracle-base]: `src/py/vertebra/extensions/django/db/backends/oracle/base.py` — Driver-free Django Oracle `BaseDatabaseWrapper` subclass.
[^django-spanner-base]: `src/py/vertebra/extensions/django/db/backends/spanner/base.py` — Django Spanner `BaseDatabaseWrapper` requiring `django-google-spanner`.
[^flask-cache-py]: `src/py/vertebra/extensions/flask/cache.py` — Lazy `FlaskCachingBackend` subclassing `flask_caching.backends.base.BaseCache`.
[^starlette-plugin-py]: `src/py/vertebra/extensions/starlette/plugin.py` — Starlette `VertebraPlugin.init_app` idempotency guard.
[^fastapi-plugin-py]: `src/py/vertebra/extensions/fastapi/plugin.py` — FastAPI `VertebraPlugin.init_app` idempotency guard.
[^queue-ring-rs]: `src/rs/vertebra-py/src/queue_ring.rs` — `ShmQueueRing` POSIX shared-memory path hashing and capacity bounds.
[^pyproject-toml]: `pyproject.toml` — Optional host extra dependencies for framework extensions.
[^litestar-vite-plugin-py]: `src/py/vertebra/extensions/litestar/vite/plugin.py` — Slotted `VertebraVitePlugin(VitePlugin)` exposing `client_aliases`.
