---
type: Knowledge
title: Advanced Alchemy & Cache Limitations
description: Operational boundaries for SQLAlchemy query_cache_size, ShmCache 128-byte key and 856-byte value limits, missing SHM invalidation rings, and Advanced Alchemy git pin caveats.
status: stable
tags:
  - domains
  - extensions
  - alchemy
  - cache
  - limitations
generated:
  by: flow/0.30.1
  at: "2026-11-17T02:30:00Z"
last_reviewed_commit: 5a833832
verified:
  - by: agent:flow-executor
    at: "2026-11-17T02:30:00Z"
    commit: 5a833832
sources:
  - id: src-pyproject
    resource: pyproject.toml
    title: Advanced Alchemy dependency floor and git commit pin
  - id: src-rs-broker
    resource: src/rs/vertebra-core/src/broker.rs
    title: Native SHM_CACHE_MAX_KEY_BYTES (128) and SHM_CACHE_MAX_VALUE_BYTES (856) constants
  - id: src-cache-sync
    resource: src/py/vertebra/cache/_sync.py
    title: SyncCacheStore BLAKE2b key hashing and SHM L1 eviction on oversized values
  - id: src-extensions-alchemy-cache-py
    resource: src/py/vertebra/extensions/alchemy/cache.py
    title: SyncCacheRegion, AsyncCacheRegion, and create_cache_config region factory
  - id: src-extensions-alchemy-plugin-py
    resource: src/py/vertebra/extensions/alchemy/plugin.py
    title: Framework-aware Alchemy config factories and RoutingConfig passthrough
  - id: src-extensions-alchemy-init-py
    resource: src/py/vertebra/extensions/alchemy/__init__.py
    title: Lazy exports for R2.09 Advanced Alchemy main surface
  - id: src-extensions-sqlalchemy-pg-py
    resource: src/py/vertebra/extensions/sqlalchemy/dialects/postgresql.py
    title: PostgreSQL +vtb dialect cast-rendering colspecs and BindTyping.RENDER_CASTS
  - id: src-extensions-sqlalchemy-types-py
    resource: src/py/vertebra/extensions/sqlalchemy/types.py
    title: Custom PgVector SQLAlchemy UserDefinedType definition
  - id: src-extensions-sqlalchemy-duckdb-py
    resource: src/py/vertebra/extensions/sqlalchemy/dialects/duckdb.py
    title: DuckDB +vtb dialect bridging duckdb_engine onto Vertebra DBAPI
  - id: src-extensions-sqlalchemy-mysql-py
    resource: src/py/vertebra/extensions/sqlalchemy/dialects/mysql.py
    title: MySQL +vtb dialect error extraction, charset detection, and disconnect checks
  - id: src-extensions-sqlalchemy-mssql-py
    resource: src/py/vertebra/extensions/sqlalchemy/dialects/mssql.py
    title: SQL Server +vtb dialect version lookup, OUTPUT nextset, and disconnect checks
---

# Advanced Alchemy & Cache Limitations

<!-- truth: start -->
- SQLAlchemy compiled statement caching stays process-local via `query_cache_size` because SQLAlchemy provides no cross-process compiled-statement or result-cache hook [^src-extensions-alchemy-cache-py].
- `ShmCache` enforces `128`-byte key (`SHM_CACHE_MAX_KEY_BYTES`) and `856`-byte value (`SHM_CACHE_MAX_VALUE_BYTES`) limits; `vertebra.cache` hashes oversized keys with `blake2b` and evicts stale SHM L1 entries on oversized writes [^src-rs-broker] [^src-cache-sync].
- Python cache stores in `vertebra.cache` and `alchemy/cache.py` do not use `ShmLeaseTable` invalidation rings; cross-process L1 coherency depends on TTL expiry or explicit `delete` / `clear` calls [^src-cache-sync] [^src-extensions-alchemy-cache-py].
- `pyproject.toml` pins `advanced-alchemy` to commit `f09816bf5647695a8d6e940ee4633caddab44d64` under `[tool.uv.sources]` while retaining the `>=1.12.0` dependency floor until that upstream release publishes [^src-pyproject].
- Upstream `RoutingAsyncSession.__init__` at `f09816bf` sets `self.bind = None` before `super().__init__(...)`, raising `AttributeError`; `RoutingAsyncSessionMaker` listeners bind through `sync_session_class` [^src-extensions-alchemy-plugin-py].
- SQLAlchemy's `util.get_cls_kwargs()` inspects positional-or-keyword `__init__` arguments only, so `BrokerPool` and `AsyncBrokerPool` declare `pool_size` and `max_overflow` before `*`; `BigQuery` and `Spanner` still override `get_pool_class` independently of `_base.py` until their dialect tasks align them [^src-extensions-alchemy-plugin-py].
- `PgVector` in `types.py` defines `__slots__ = ("column_name", "dimensions")` without a custom `adapt()` method, so SQLAlchemy's `constructor_copy()` inside `_PGVectorCast` bind-cast adaptation resets `dimensions` to `None` on `type_descriptor(PgVector(dim))` until `types.py` adds `render_bind_cast = True` or an `adapt()` override [^src-extensions-sqlalchemy-pg-py] [^src-extensions-sqlalchemy-types-py].
- `duckdb_engine.Dialect` routes `connect()` directly to `duckdb.connect`, accesses `.closed` and `.begin()` missing on `AsyncAdapt_vertebra_connection`, inspects `cursor.connection.notices` in `PGExecutionContext_psycopg2`, and SQLAlchemy `URL.render_as_string()` percent-encodes `:memory:` as `%3Amemory%3A`; `dialects/duckdb.py` overrides these hooks and unquotes `/%3Amemory%3A` so `:memory:` stays in-memory on `vertebra._duckdb` [^src-extensions-sqlalchemy-duckdb-py].
- `BaseMySQLDialect.initialize()` calls `_detect_charset()`, which raises `NotImplementedError` on SQLAlchemy's base MySQL dialect; `_MySQLDialectMixin` provides `_detect_charset()` alongside `_extract_error_code()` so `mysql+vtb://` and `mysql+vtb_async://` initialize cleanly without a `pymysql` driver [^src-extensions-sqlalchemy-mysql-py].
- `MsgspecJSON` overrides `bind_processor()` and `result_processor()` to bypass `sqltypes.JSON`'s `json.dumps`/`json.loads` wrappers when delegating DDL and `jsonb()`/`json()` SQL expressions to `sqlite.JSONB()`, `postgresql.JSONB()`, and `oracle.JSON()` [^src-extensions-sqlalchemy-types-py].
- `alembic/impl.py` calls `import_optional("sqlalchemy_bigquery.base")` before defining `BigQueryAlembicImpl` so `sqlalchemy_bigquery`'s own `SqlalchemyBigqueryImpl` (`__dialect__ = "bigquery"`) cannot overwrite `BigQueryAlembicImpl` in `alembic.ddl.impl._impls` on lazy dialect import.
<!-- truth: end -->

## Operational Boundaries

- **Process-Local `query_cache_size`**: SQLAlchemy caches compiled SQL constructs per engine inside each Python worker process via `query_cache_size`. Vertebra does not serialize compiled SQLAlchemy statements or query results into shared memory.
- **`ShmCache` Key and Value Size Bounds**: Native [`broker.rs`](../../../../../src/rs/vertebra-core/src/broker.rs)[^src-rs-broker] limits shared-memory entries to `128`-byte keys (`SHM_CACHE_MAX_KEY_BYTES`) and `856`-byte values (`SHM_CACHE_MAX_VALUE_BYTES`). [`SyncCacheStore`](../../../../../src/py/vertebra/cache/_sync.py)[^src-cache-sync] and `AsyncCacheStore` hash keys above `128` UTF-8 bytes with `hashlib.blake2b` while keeping a bounded namespace prefix. When a payload exceeds `856` bytes, SHM `set()` deletes any stale entry and `add()` returns `False`, while L1 `shm://` stores evict the key and serve from L2.
- **No `ShmLeaseTable` Cache Invalidation Rings**: Neither [`vertebra.cache`](../../../../../src/py/vertebra/cache/__init__.py) nor [`alchemy/cache.py`](../../../../../src/py/vertebra/extensions/alchemy/cache.py)[^src-extensions-alchemy-cache-py] attaches `ShmLeaseTable` invalidation rings to Python cache stores. When pairing a process-local `memory://` L1 with a shared L2 backend (`valkey://`, `redis://`, or `shm://`), tune L1 `default_ttl` or issue explicit `delete` / `clear` calls.
- **R2.09 `advanced-alchemy` Git Pin vs. Floor**: [`pyproject.toml`](../../../../../pyproject.toml)[^src-pyproject] pins `advanced-alchemy` under `[tool.uv.sources]` to commit `f09816bf5647695a8d6e940ee4633caddab44d64` to expose `ServiceGroup`, `FilterSet`, `RelationshipFilter`, `SchemaDumpConfig`, `SQLAlchemyStore.delete_expired`, and Spanner/CockroachDB `IntegrityError` mapping. Published extras keep `"advanced-alchemy>=1.12.0"` until the upstream release lands; offline sandboxes that cannot re-lock `uv.lock` install the pinned wheel via `uv pip install --no-deps` and verify with `uv run --no-sync`.
- **Upstream `RoutingAsyncSession` Caveat (`f09816bf`)**: At pinned commit `f09816bf`, `RoutingAsyncSession.__init__` assigns `self.bind = None` before calling `super().__init__(...)`, which triggers `AttributeError` on direct instantiation. Routing session maker listeners still attach via `RoutingAsyncSessionMaker._session_config["sync_session_class"]` and work for synchronous `RoutingSyncSessionMaker` sessions via [`create_sync_alchemy_config`](../../../../../src/py/vertebra/extensions/alchemy/plugin.py)[^src-extensions-alchemy-plugin-py] and [`alchemy/__init__.py`](../../../../../src/py/vertebra/extensions/alchemy/__init__.py)[^src-extensions-alchemy-init-py].
- **`PgVector` `__slots__` Adaptation Caveat**: Because [`PgVector`](../../../../../src/py/vertebra/extensions/sqlalchemy/types.py)[^src-extensions-sqlalchemy-types-py] declares `__slots__ = ("column_name", "dimensions")` without overriding `UserDefinedType.adapt()`, SQLAlchemy's `util.constructor_copy` reads `__dict__` when adapting `PgVector(dim)` to [`_PGVectorCast`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects/postgresql.py)[^src-extensions-sqlalchemy-pg-py] and drops `dimensions` (`None`) on adapted instances until `types.py` sets `render_bind_cast = True` directly or overrides `adapt()`.
- **`duckdb_engine` DBAPI & `:memory:` Adaptation Caveat**: [`duckdb.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects/duckdb.py)[^src-extensions-sqlalchemy-duckdb-py] requires `duckdb_engine` via `ensure_module` and overrides `connect()`, `do_begin()`, `do_rollback()`, `do_ping()`, `create_connect_args()`, and `DuckDBExecutionContext._log_notices()` because `duckdb_engine.Dialect` hardcodes `duckdb.connect`, `dbapi_connection.closed` / `.begin()`, and `PGExecutionContext_psycopg2` notice inspection, while SQLAlchemy `URL.render_as_string()` percent-encodes `:memory:` into `/%3Amemory%3A` which would otherwise open an on-disk database file.
- **SQL Server & MySQL Dialect Hooks**: [`mssql.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects/mssql.py)[^src-extensions-sqlalchemy-mssql-py] bounds `nextset()` retries in `MSExecutionContext_vtb.post_exec()` to 16 iterations and falls back from `SERVERPROPERTY('ProductVersion')` to `@@VERSION`; [`mysql.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects/mysql.py)[^src-extensions-sqlalchemy-mysql-py] implements `_detect_charset()` defaulting to `utf8mb4` when a driver does not expose `character_set_name()`.

[^src-pyproject]: `pyproject.toml` — Advanced Alchemy `>=1.12.0` floor and `[tool.uv.sources]` commit pin `f09816bf5647695a8d6e940ee4633caddab44d64`.
[^src-rs-broker]: `src/rs/vertebra-core/src/broker.rs` — Native `SHM_CACHE_MAX_KEY_BYTES` (`128`) and `SHM_CACHE_MAX_VALUE_BYTES` (`856`) bounds.
[^src-cache-sync]: `src/py/vertebra/cache/_sync.py` — `SyncCacheStore` BLAKE2b key hashing and SHM L1 eviction on oversized values.
[^src-extensions-alchemy-cache-py]: `src/py/vertebra/extensions/alchemy/cache.py` — `SyncCacheRegion`, `AsyncCacheRegion`, and `create_cache_config` region factory.
[^src-extensions-alchemy-plugin-py]: `src/py/vertebra/extensions/alchemy/plugin.py` — Framework-aware Alchemy config factories and `RoutingConfig` passthrough.
[^src-extensions-alchemy-init-py]: `src/py/vertebra/extensions/alchemy/__init__.py` — Lazy exports for R2.09 Advanced Alchemy `main` surface.
[^src-extensions-sqlalchemy-pg-py]: `src/py/vertebra/extensions/sqlalchemy/dialects/postgresql.py` — PostgreSQL `+vtb` dialect cast-rendering `colspecs` and `BindTyping.RENDER_CASTS`.
[^src-extensions-sqlalchemy-types-py]: `src/py/vertebra/extensions/sqlalchemy/types.py` — Custom `PgVector` SQLAlchemy `UserDefinedType` definition.
[^src-extensions-sqlalchemy-duckdb-py]: `src/py/vertebra/extensions/sqlalchemy/dialects/duckdb.py` — DuckDB `+vtb` dialect bridging `duckdb_engine` onto Vertebra DBAPI.
[^src-extensions-sqlalchemy-mysql-py]: `src/py/vertebra/extensions/sqlalchemy/dialects/mysql.py` — MySQL `+vtb` dialect error extraction, charset detection, and disconnect checks.
[^src-extensions-sqlalchemy-mssql-py]: `src/py/vertebra/extensions/sqlalchemy/dialects/mssql.py` — SQL Server `+vtb` dialect version lookup, `OUTPUT` `nextset`, and disconnect checks.
