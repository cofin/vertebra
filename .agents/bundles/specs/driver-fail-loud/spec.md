---
type: Spec
flow_id: driver-fail-loud
title: Fail-Loud Database Drivers
description: Replace the Oracle, MSSQL, and DuckDB SQLite stand-ins, the SQLAlchemy SQLite parse fallback, the in-memory queue executors and clients, and memory:// engine fakes with MissingDependencyError or configuration errors, delete dead vertebra-db pools and superseded codecs, correct install hints behind an extras gate, make conformance outcomes explicit when a native module is absent, and align user docs, agent instructions, and OKF knowledge with the result.
status: stable
state: in_progress
has_chapters: true
chapters:
  - ch01-oracle-mssql-fail-loud
  - ch02-duckdb-sqlalchemy-queue-fail-loud
  - ch03-remove-memory-fakes
  - ch04-redis-valkey-memory-removal
  - ch05-mssql-codec-cleanup
  - ch06-oracle-codec-cleanup
  - ch07-vertebra-db-pools-and-mysql
  - ch08-install-hints-and-extras-gate
  - ch09-conformance-outcomes
  - ch10-queue-backend-fail-loud
  - ch11-docs-and-okf
  - ch12-queue-docs-and-knowledge
  - ch13-agent-knowledge-and-embeddings-docs
current_chapter: ch04-redis-valkey-memory-removal
current_task: "driver-fail-loud:ch04:t01"
parent_prd: native-data-platform
research:
  - research/driver-restructure_20261107.md
  - research/change-surface_20261107.md
plan_revision: 2
plan_commit: fd571e3a
state_revision: 3
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-09T03:21:00Z"
verified:
  - by: agent:code-reviewer
    at: "2026-11-08T00:00:00Z"
    commit: 1e1f028a
generated:
  by: flow/0.28.0
  at: "2026-11-07T00:00:00Z"
sources:
  - id: module-loader
    resource: src/py/vertebra/utils/module_loader.py::ensure_module
    title: Optional module loader raising MissingDependencyError
  - id: missing-dep
    resource: src/py/vertebra/_typing.py::MissingDependencyError
    title: Missing dependency error and install hint
  - id: oracle-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: Oracle driver backed by sqlite3
  - id: mssql-driver
    resource: src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver
    title: MSSQL driver backed by sqlite3
  - id: duckdb-driver
    resource: src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver
    title: DuckDB driver with sqlite3 fallback
  - id: dep-groups
    resource: pyproject.toml::dependency-groups
    title: Dependency groups; duckdb only in bench
  - id: sqla-dbapi
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection
    title: SQLAlchemy DB-API connection with SQLite parse fallback
  - id: oracle-queue
    resource: src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend
    title: Oracle queue backend with in-memory executor fallback
  - id: pg-queue
    resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
    title: PostgreSQL queue backend with in-memory executor fallback
  - id: redis-queue
    resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend
    title: Redis and Valkey queue backend with in-memory stream client fallback
  - id: spanner-queue
    resource: src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend
    title: Spanner queue backend with in-memory executor fallback
  - id: redis-config
    resource: src/py/vertebra/db/adapters/redis/config.py::RedisConfig
    title: Redis config accepting the memory scheme
  - id: litestar-store
    resource: src/py/vertebra/extensions/litestar/store.py::MemoryStore
    title: Litestar store with from_valkey and from_redis factories
  - id: db-lib
    resource: src/rs/vertebra-db/src/lib.rs
    title: vertebra-db re-exports of pools and codecs
  - id: db-bindings
    resource: src/rs/vertebra-py/src/db.rs::register_db_bindings
    title: Python codec bindings on vertebra._vertebra
  - id: conformance
    resource: src/py/tests/integration/db/test_driver_conformance.py::_resolve_live_config
    title: Conformance live config resolution
  - id: packaging-test
    resource: src/py/tests/unit/build/test_packaging_and_pgo.py
    title: Packaging extras assertions
tags:
  - database
  - drivers
  - fail-loud
  - cleanup
  - packaging
  - queues
---

# Flow: Fail-Loud Database Drivers

<!-- truth: start -->
- **Native module guard**: Oracle and MSSQL drivers resolve `vertebra._oracle` / `vertebra._mssql` through `ensure_module` in every entry point and raise `MissingDependencyError` until the native wheels ship.[^module-loader][^oracle-driver][^mssql-driver]
- **No SQLite substitution**: no engine other than SQLite opens a `sqlite3` connection; DuckDB requires the `duckdb` package and the SQLAlchemy DB-API raises `InterfaceError` on unparsable URLs.[^duckdb-driver][^sqla-dbapi]
- **DuckDB tests by interpreter**: `duckdb` lives in a dedicated dependency group installed on GIL interpreters; free-threaded runs skip DuckDB tests through `pytest.importorskip`.[^dep-groups]
- **No in-process engine fakes**: PostgreSQL, MySQL, Redis, and Valkey drivers reject `memory://`; Oracle, PostgreSQL, and Spanner queue backends require an injected executor and Redis/Valkey queue backends a live client, raising otherwise.[^redis-config][^oracle-queue][^pg-queue][^redis-queue][^spanner-queue]
- **vertebra-db holds live code only**: unused `*Pool` lease tables and codecs superseded by engine client crates are deleted; codecs with current Python callers remain.[^db-lib][^db-bindings]
- **Truthful install hints**: `MissingDependencyError` defaults its hint to `vertebra-core[<extra>]`; the extras gate requires every `extra=` literal in `src/py/vertebra` to be a declared extra; Oracle and MSSQL guards pass `install_package="vertebra-core[oracle]"` / `"vertebra-core[mssql]"`, naming the planned native extras.[^missing-dep][^packaging-test]
- **Explicit conformance outcome**: a missing native module skips its case; `VERTEBRA_CONFORMANCE_DOCKER=1` fails the case only for engines without `pending_flow_id`; strict `xfail` applies only when a pending engine's module imports.[^conformance]
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Vertebra's Oracle and MSSQL drivers accept host, user, and password, then execute every statement against an in-memory SQLite database. DuckDB silently becomes SQLite when the `duckdb` package is absent, and the SQLAlchemy DB-API becomes an in-memory SQLite connection when a URL fails to parse. The Oracle, PostgreSQL, and Spanner queue backends substitute in-process executors and the Redis/Valkey queue backend an in-process stream client when no live collaborator is supplied. PostgreSQL, MySQL, Redis, and Valkey drivers accept `memory://` and return rows from a dictionary. Each path returns plausible results from the wrong engine.

`vertebra-db` carries connectionless `*Pool` lease tables and MySQL, TDS, and TNS framing that the selected client crates (`mysql_async`, `tiberius`, `oracledb`) replace. Install hints point at `vtb[...]` and at extras that `pyproject.toml` does not declare. The conformance suite errors, or hides failures behind strict xfail, when a native module is absent.

This flow makes every unsupported path raise, deletes dead native code, corrects install hints, makes conformance outcomes explicit, and brings user docs and OKF knowledge in line, so the per-engine native driver flows start from a truthful baseline.

**Non-goals** (each with its owner):

- Native engine clients and per-engine optional-dependency extras (`postgres`, `mysql`, `oracle`, `mssql`, `sqlite`, `duckdb`, `redis`, `valkey`). Owner: each `native-<engine>-driver` flow.
- A DataFusion optional extra and a native DataFusion plan. Owner: a `native-data-platform` follow-up opened with `/flow:plan`.
- Registry entry-point discovery for optional driver wheels. Owner: a `native-data-platform` follow-up opened with `/flow:plan`.
- Live PostgreSQL and Oracle queue executors (`PostgresQueueStore`, `OracleQueueStore`). Owner: `queues-tasks-and-channels`.
- A live Spanner queue executor. Owner: a follow-up opened with `/flow:plan spanner-queue-executor`.
- `valkey://` / `valkeys://` URL support in the Redis queue backend. Owner: a follow-up spec recorded in `ch10 / t02` Notes.
- Pure-Python TDS/TNS framing in `mssql/core.py` / `oracle/core.py`. Owner: `native-mssql-driver` and `native-oracle-driver`.
- RESP3 codec removal. Owner: `native-redis-driver`.
- The SQLAlchemy second `sqlite3` connection for genuine SQLite URLs, DB-API error mapping beyond the parse fallback, and the Django `select_to_arrow(rows=)` passthrough. Owner: `sqlalchemy-and-django-integration`.
- Unused `vertebra-db` Cargo dependencies beyond `sha2` and `rmp-serde`. Owner: a `cargo machete` follow-up opened with `/flow:plan`.
- A free-threaded DuckDB build (cached source build). Owner: a follow-up opened with `/flow:plan` once DuckDB publishes free-threaded wheels.
- A repo-wide `from __future__ import annotations` sweep. Owner: a separate cleanup spec opened with `/flow:plan`.

### 1.2 Upstream Execution Baseline

- Branch `feat/storage-updates` at `fd571e3a` (stacked immediately after [`storage-remediation`](../storage-remediation/spec.md)); parent PRD [`native-data-platform`](../native-data-platform/spec.md).
- Runs ahead of `native-postgres-driver`, `broker-pool-sharing`, and the per-engine driver flows, which consume its baseline.

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Optional modules | `src/py/vertebra/utils/module_loader.py::ensure_module`[^module-loader], `src/py/vertebra/_typing.py::MissingDependencyError`[^missing-dep] | In-process; cached import with typed error and install hint |
| Oracle / MSSQL | `oracle/driver.py::SyncOracleDriver`[^oracle-driver], `mssql/driver.py::SyncMSSQLDriver`[^mssql-driver] | External boundary (native module); consumed lazily by registry, SQLSpec adapters, Django backends |
| DuckDB | `duckdb/driver.py::SyncDuckDBDriver`[^duckdb-driver], `pyproject.toml` `[dependency-groups]`[^dep-groups], `Makefile::py-test` | External boundary (`duckdb` package); no free-threaded wheel |
| SQLAlchemy DB-API | `extensions/sqlalchemy/dialects.py::SyncDBAPIConnection`[^sqla-dbapi] | In-process over `DatabaseRegistry.parse_url` |
| Queue backends | `framework/queues/oracle.py::OracleQueueBackend`[^oracle-queue], `postgres.py::PostgreSQLQueueBackend`[^pg-queue], `redis.py::RedisQueueBackend`[^redis-queue], `spanner.py::SpannerQueueBackend`[^spanner-queue] | External boundary (`oracledb`, `psycopg`, `redis`, `vertebra._spanner`); `executor=` / `client=` are the injected seams |
| Engine fakes | `redis/config.py::RedisConfig`[^redis-config], `postgres/driver.py`, `mysql/driver.py`, `redis/driver.py`, `extensions/litestar/store.py::MemoryStore`[^litestar-store], `extensions/starlette/store.py::CacheStore`, `framework/stores.py::KeyValueStore` | Local-substitutable fakes replaced by injected `connect_factory` / `pool` doubles and a loopback RESP3 server |
| Native codecs | `vertebra-db/src/lib.rs`[^db-lib], `vertebra-py/src/db.rs::register_db_bindings`[^db-bindings], `_vertebra.pyi` | In-process Rust; stubtest-gated Python surface |
| Gates | `test_packaging_and_pgo.py`[^packaging-test], `test_driver_conformance.py::_resolve_live_config`[^conformance] | Test seams |

Full symbol inventory: [change-surface research](research/change-surface_20261107.md).

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Database drivers and dialers](../../knowledge/domains/data/database-drivers-and-dialers.md)
- [Query pipeline, CDC, and tenancy](../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md)
- [Distributed queues and email](../../knowledge/domains/framework/distributed-queues-and-email.md)
- [ADR-004](../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md)
- [Project workflow](../../knowledge/workflow.md)
- [Skills index](../../../skills/index.md)

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R1**: Constructing `SyncOracleDriver`, `AsyncOracleDriver`, `SyncMSSQLDriver`, or `AsyncMSSQLDriver` raises `MissingDependencyError` for `vertebra._oracle` / `vertebra._mssql`; no Oracle or MSSQL code path opens `sqlite3`. The public driver members `set_session_attrs`, `last_piggyback_frame`, `last_rewritten_sql`, and `last_tds_batch` are deleted.
- **R2**: `AsyncMSSQLDriver(config, *, dialer: SyncCloudSQLDialer | None = None, ...)` forwards `dialer` to its `SyncMSSQLDriver`; `MSSQLConfig.build_async_driver` requests `registry.get_dialer(cfg, sync=True)`.
- **R3**: `SyncDuckDBDriver` / `AsyncDuckDBDriver` raise `MissingDependencyError` when `duckdb` is absent. `duckdb` lives in its own `[dependency-groups].duckdb` group, installed unconditionally by `make install` and by `make py-test`, `make coverage`, and `make upgrade` on GIL interpreters only (so CI non-free-threaded jobs install it); on free-threaded interpreters DuckDB tests skip through `pytest.importorskip("duckdb", reason=...)`.
- **R4**: `SyncDBAPIConnection` and `AsyncDBAPIConnection` raise the DB-API `InterfaceError`, chained from the parse exception, when `DatabaseRegistry.parse_url` fails.
- **R5**: `OracleQueueBackend` binds an injected `executor=`; without one it calls `ensure_module("oracledb")`, raising `MissingDependencyError` when the package is absent and `ImproperConfigurationError` when it is present. Canonical test: `src/py/tests/unit/framework/test_distributed_queues.py`.
- **R6**: PostgreSQL and MySQL drivers reject `memory://` DSNs with `ImproperConfigurationError`; `_in_memory_rows` and every memory branch are deleted; a PostgreSQL pool exposing no execution method raises `ImproperConfigurationError`.
- **R7**: Redis and Valkey lose their `memory://` emulation.
  - **R7a**: A threaded loopback RESP3 server fixture (`src/py/tests/unit/conftest.py::resp3_server`) replaces every `memory://` Redis or Valkey construction in the driver, store, Advanced Alchemy, Starlette, and framework unit tests.
  - **R7b**: `RedisConfig.url_schemes` excludes `memory`, so `DatabaseRegistry.parse_url("memory://")` raises `ValueError`; the remaining cache tests use the fixture or live-only conformance resolution.
  - **R7c**: Redis and Valkey drivers delete memory execution and raise `ImproperConfigurationError` (a `ValueError` subclass) for `memory://`, which propagates through `MemoryStore`, `CacheStore`, `KeyValueStore`, and `SQLAlchemyCacheBackend` `from_valkey` / `from_redis`; `SQLAlchemyCacheBackend.from_url("memory://")` stays local.
- **R8**: The six `*Pool` types, the `mysql` module, Rust-only TDS/TNS framing, unused dialect tags, `OracleWalletMaterial` (the whole struct, including `from_directory`), and the `sha2` and `rmp-serde` dependencies are deleted from `vertebra-db`; TNS descriptor parsing (`parse_tnsnames_ora`, `resolve_tns_alias`) stays, and Python wallet handling stays in `oracle/config.py::OracleWallet`.
- **R9**: The `encode_tds_sql_batch` binding, its stub, its Python wrappers (`MSSQLCore.encode_tds_sql_batch`, `MSSQLConfig.encode_sql_batch`, the package export), and Python branches probing unexported natives are deleted; crate, module, and adapter docs describe codecs, not drivers.
- **R10**: `MissingDependencyError` defaults its hint to `vertebra-core[<extra>]`; every `extra=` literal in `src/py/vertebra` is declared in `[project.optional-dependencies]`; sentence-valued `package=` arguments become package names.
- **R11**: A unit test fails when `src/py/vertebra` references an undeclared extra, a sentence-valued package, or a bracketed Vertebra distribution other than `vertebra-core`.
- **R12**: Conformance resolves the native module before credential skips. A missing module skips its case locally; under `VERTEBRA_CONFORMANCE_DOCKER=1` it still skips engines with `pending_flow_id` set and fails engines without it (`bigquery`, `spanner`, and any engine whose native module has shipped). Strict `xfail` applies only when a pending engine's module imports. The DuckDB case stays pending on `vertebra._duckdb`.
- **R13**: README, database user docs, and OKF data, testing, product, and decision knowledge describe Oracle and MSSQL as pending native drivers whose packages are not yet published, DuckDB as requiring `duckdb`, the current codec inventory, install hints, and conformance outcomes.
- **R14**: `PostgreSQLQueueBackend` binds an injected `executor=`; without one it raises `MissingDependencyError` (`install_package="psycopg[binary,pool]"`) when `psycopg` is absent and `ImproperConfigurationError` when it is present.
- **R15**: `RedisQueueBackend` and `ValkeyQueueBackend` bind an injected `client=`; without one they raise `MissingDependencyError` (`install_package="redis"`) when `redis.asyncio` is absent, `ImproperConfigurationError` for a `backend_url` without a hostname, and otherwise bind `redis.asyncio.from_url(backend_url)`.
- **R16**: `SpannerQueueBackend` binds an injected `executor=`; without one it raises `MissingDependencyError` (`install_package="vertebra-core[spanner]"`) when `vertebra._spanner` is absent and `ImproperConfigurationError` when it is present.
- **R17**: The queues usage guide, queues reference, and OKF distributed-queues chapter state the executor-or-raise rule (Oracle, PostgreSQL, Spanner), the client-or-raise rule (Redis/Valkey), and each backend's install package, and drop claims the code does not back.
- **R18**: Agent instructions, OKF knowledge, and user docs outside the database and queue pages match the baseline.
  - **R18.1**: `AGENTS.md`, `workflow.md`, and `benchmarks-and-free-threaded.md` state the `make install` command with `--group duckdb`, the GIL-only `--group duckdb` on `py-test`, `coverage`, and `upgrade`, and the dedicated `duckdb` group that `bench` includes and `dev` excludes.
  - **R18.2**: `tech-stack.md` drops `sha2` and `rmp-serde` from `vertebra-db` and lists the `duckdb` group; `polyglot-test-matrix.md` adds the GIL-only DuckDB group, the `pytest.importorskip("duckdb")` rule, and the `resp3_server` fixture.
  - **R18.3**: `sqlspec-alchemy-and-dialects.md` states the DB-API `InterfaceError` and Oracle/MSSQL `MissingDependencyError`; `domains/index.md` marks the Oracle and MSSQL drivers and `OracleEmbeddingPushdown` as pending; `docs/index.md` drops its engine count.
  - **R18.4**: `docs/usage/embeddings-and-vectors.md`, `docs/usage/index.md`, and the OKF embeddings chapter state that `OracleEmbeddingPushdown` needs the pending native Oracle driver and that constructing an Oracle driver raises `MissingDependencyError`.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
def _require_oracle_native() -> ModuleType:
    """Resolve the native Oracle module.

    Raises:
        MissingDependencyError: When ``vertebra._oracle`` is not installed.
    """
    return cast(
        "ModuleType",
        ensure_module("vertebra._oracle", package="vertebra-core[oracle]", install_package="vertebra-core[oracle]"),
    )
```

`_require_mssql_native()` mirrors it for `vertebra._mssql` / `vertebra-core[mssql]`. Every public method of the Oracle and MSSQL drivers calls the resolver first; `__init__` calls it before any other state is built. Driver classes stay concrete subclasses of `SyncDatabaseDriver` / `ThreadOffloadAsyncDatabaseDriver` so instantiation raises `MissingDependencyError`, never `TypeError`. The hint names the planned native extra through `install_package=`, not `extra=`, so the extras gate (which checks `extra=` literals) passes while `oracle` / `mssql` stay undeclared.

```python
class MissingDependencyError(VertebraError, ImportError):
    def __init__(self, package: str, *, install_package: str | None = None, extra: str | None = None) -> None: ...
```

Default `install_package` becomes `f"vertebra-core[{extra}]"` when `extra` is set, else `package`; `hint` is `f"Install it with: uv add {install_package}"`.

Queue backend constructors keep their signatures; the injected seams are `OracleQueueBackend(config, *, oracle_config=None, executor: OracleQueueExecutor | None = None)`, `PostgreSQLQueueBackend(config, *, postgres_config=None, executor=None)`, `RedisQueueBackend(config, *, client=None)`, and `SpannerQueueBackend(config, *, executor=None)`; exact signatures live in [ch02](chapters/ch02-duckdb-sqlalchemy-queue-fail-loud/spec.md) and [ch10](chapters/ch10-queue-backend-fail-loud/spec.md).

`pyproject.toml` declares `duckdb = ["duckdb>=1.0"]`, included by `bench` and not by `dev`.[^dep-groups] The `Makefile` selects the group per interpreter:

```make
PY_GIL_DISABLED = $(shell uv run --no-sync python -c "import sysconfig; print(int(bool(sysconfig.get_config_var('Py_GIL_DISABLED'))))" 2>/dev/null)
PY_TEST_GROUPS = $(if $(filter 0,$(PY_GIL_DISABLED)),--group duckdb)
unexport PY_GIL_DISABLED PY_TEST_GROUPS
```

`PY_TEST_GROUPS` is `--group duckdb` only when the probe prints exactly `0`; `1`, empty output, or a failed probe adds no group. `unexport` keeps both variables out of recipe environments. Reason: the `Makefile` declares `.EXPORT_ALL_VARIABLES:`, which would otherwise run the probe in every recipe. `install` syncs `--group duckdb` unconditionally; `py-test`, `coverage`, and `upgrade` use `$(PY_TEST_GROUPS)`.

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `DatabaseRegistry.get_shared_driver(config)` → `DatabaseRegistry._build_async_driver` → `OracleConfig.build_async_driver(registry)` (inherited from `BaseDatabaseConfig`) → `AsyncOracleDriver.__init__` → `SyncOracleDriver.__init__` → `_require_oracle_native()` → `ensure_module` → `MissingDependencyError`. `DatabaseRegistry.get_shared_sync_driver(config)` → `_build_sync_driver` → `OracleConfig.build_sync_driver(registry)` → `SyncOracleDriver.__init__` reaches the same guard.
- `MSSQLConfig.build_async_driver(registry)` → `registry.get_dialer(cfg, sync=True)` → `AsyncMSSQLDriver(cfg, dialer=...)` → `SyncMSSQLDriver(cfg, dialer=...)` → `_require_mssql_native()`.
- `extensions/sqlspec/adapters.py` session provider → driver class constructor → same guard; config construction stays import-safe.
- `SyncDBAPIModule().connect(url)` → `SyncDBAPIConnection.__init__` → `DatabaseRegistry.parse_url` → `ValueError` → `InterfaceError` raised `from` it.
- `QueueService(QueueConfig(backend_url=...))` → `QueueService._build_backend` → queue backend constructor → injected `executor=` / `client=`, else `ensure_module(...)` → `MissingDependencyError` or `ImproperConfigurationError`.
- `MemoryStore.from_valkey("memory://")` → `ValkeyConfig(url="memory://")` → `AsyncValkeyDriver.__init__` → `_reject_memory_url` → `ImproperConfigurationError`.
- `make py-test` → `PY_TEST_GROUPS` (`--group duckdb` only when the probe prints `0`) → `uv run $(PY_TEST_GROUPS) pytest` → DuckDB tests run, or skip via `pytest.importorskip("duckdb")`.
- `_resolve_live_config(spec, tmp_path, request)` → `_require_native_module(spec)` → `pytest.skip`, or `pytest.fail` when `spec.pending_flow_id is None` and `VERTEBRA_CONFORMANCE_DOCKER == "1"`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario | Trigger | Expected Behavior | Invariant |
| --- | --- | --- | --- |
| Registry import | `import vertebra.db.registry` | No driver module import, no `ensure_module` call | `test_registry_parse_url_executes_zero_eager_driver_imports` stays green |
| SQLSpec Oracle config | Construct `OracleSyncConfig` | Succeeds; session acquisition raises `MissingDependencyError` | Config import safety |
| Async Oracle driver | `AsyncOracleDriver(config)` | `MissingDependencyError` before thread offload state | No `TypeError` |
| DuckDB absent | `duckdb` import fails | `MissingDependencyError("duckdb", install_package="duckdb")` | No `sqlite3` import in DuckDB module |
| DuckDB tests, free-threaded | `3.14t` / `3.15t` interpreter | `duckdb` not installed; DuckDB tests SKIPPED | No sdist build in CI |
| Bad SQLAlchemy URL | `parse_url` raises `ValueError` | `InterfaceError` with `__cause__` set | No SQLite substitution |
| Postgres `memory://` | `PostgreSQLConfig(dsn="memory://")` driver | `ImproperConfigurationError` | No dict rows |
| Redis `memory://` | `DatabaseRegistry.parse_url("memory://")` | `ValueError` unknown scheme | Queue `memory://` and object storage `memory://` unchanged |
| Queue memory backend | `QueueConfig(backend_url="memory://")` | `MemoryQueueBackend` unchanged | Real in-process backend kept |
| Network queue without collaborator | `QueueService(QueueConfig(backend_url="postgresql://db/app"))`, no executor | `MissingDependencyError` or `ImproperConfigurationError` | No in-process executor |
| Stub drift | pyfunction removed | `_vertebra.pyi` entry removed in same commit | `mypy.stubtest` green |
| Conformance local | native module missing | `pytest.skip` | No setup ERROR |
| Conformance docker, pending engine | `VERTEBRA_CONFORMANCE_DOCKER=1`, `vertebra._postgres` missing | `pytest.skip` | Docker runs stay green until native flows ship |
| Conformance docker, non-pending engine | `VERTEBRA_CONFORMANCE_DOCKER=1`, `vertebra._bigquery` missing | `pytest.fail` | No silent pass for shipped modules |
| Conformance pending, module imports | `vertebra._postgres` present | Strict `xfail` | XPASS still fails |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given a default install, when a user builds an Oracle driver from `oracle://u:p@db:1521/svc`, then `MissingDependencyError` is raised with `exc.package == "vertebra-core[oracle]"` and `exc.hint == "Install it with: uv add vertebra-core[oracle]"`.
- Given `duckdb` installed, when `SyncDuckDBDriver(DuckDBConfig()).select("select 42 as x")` runs, then it returns `[{"x": 42}]` from DuckDB.
- Given `SyncDBAPIModule()`, when `.connect("nonsense+vtb://host/db")` runs, then `InterfaceError` is raised with a `ValueError` cause.
- Given `RedisConfig`, when `parse_url("memory://")` runs, then `ValueError` is raised.
- Given `oracledb` absent, when `QueueService(QueueConfig(backend_url="oracle://localhost:1521/FREEPDB1"))` is constructed, then `MissingDependencyError` names `oracledb`.
- Given `src/py/vertebra` with `extra="wasm"` and no `wasm` extra, when the gate runs, then it fails naming `wasm`.
- Given `vertebra._postgres` absent and `VERTEBRA_CONFORMANCE_DOCKER=1`, when the PostgreSQL conformance case runs, then it is SKIPPED, not failed.

### 2.6 Alternatives Considered

| Alternative | Depth / Locality / Seam | Outcome |
| --- | --- | --- |
| Lazy raise in a `_ensure_client()` like BigQuery | Defers failure to first query; drivers would hold half-built state | Rejected: construction is the earliest point and Oracle/MSSQL have no working path |
| `NotImplementedError` method bodies | Distinct error type per method, no install hint | Rejected: one `MissingDependencyError` path keeps callers' `except ImportError` handling uniform |
| Injectable connection port for Oracle/MSSQL tests | One adapter only (the fake) | Rejected: shallow seam; native flows define their client seam |
| Keep `memory://` as a `vertebra.testing` helper | Preserves convenience | Rejected by user; injected factories already exist |
| Interim extras installing psycopg/pymysql | Names collide with native wheel extras | Rejected: native flows own engine extras; hints name pip packages directly |
| Oracle/MSSQL hints via `extra="oracle"` | Gate would flag undeclared extras | Rejected: `install_package="vertebra-core[oracle]"` names the planned extra; docs state the packages are not yet published |
| `duckdb` in the `test` group for every interpreter | No wheel for `3.14t` / `3.15t` | Rejected: dedicated GIL-only group plus `pytest.importorskip` |
| Fail every pending conformance engine under Docker | Docker runs fail on every host until all native flows ship | Rejected: only non-pending engines fail |
| Delete all codecs including RESP3 and OSON | Smaller crate | Rejected: live Python callers and downstream flows consume them |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[5/33 files documented]` — union of the chapter documentation plans; ch03 through ch09 document only in-code docstrings that ship with each production change.
- `[x]` `src/py/vertebra/db/adapters/oracle/driver.py` — Module and class docstrings state the `vertebra._oracle` requirement and `MissingDependencyError` (ch01 / t01).
- `[x]` `src/py/vertebra/db/adapters/mssql/driver.py` — Module and class docstrings state the `vertebra._mssql` requirement; `AsyncMSSQLDriver` documents its sync dialer (ch01 / t02).
- `[x]` `src/py/vertebra/extensions/sqlalchemy/dialects.py` — `SyncDBAPIConnection.__init__` / `AsyncDBAPIConnection.__init__` `Raises: InterfaceError` (ch02 / t02).
- `[x]` `src/py/vertebra/framework/queues/oracle.py` — `OracleQueueBackend` class docstring names the required `executor`; `__init__` `Raises:` (ch02 / t03).
- `[x]` `src/py/vertebra/db/adapters/duckdb/driver.py` — `SyncDuckDBDriver.__init__` Google docstring with `Raises:` (ch02 / t04).
- `[ ]` `src/py/vertebra/framework/queues/postgres.py` — `PostgreSQLQueueBackend` class docstring and `__init__` `Args:` / `Raises:` (ch10 / t01).
- `[ ]` `src/py/vertebra/framework/queues/redis.py` — `RedisQueueBackend.__init__` `Args:` / `Raises:`; `_build_client` summary (ch10 / t02).
- `[ ]` `src/py/vertebra/framework/queues/spanner.py` — `SpannerQueueBackend.__init__` `Args:` without `driver` and `Raises:` (ch10 / t03).
- `[ ]` `src/py/vertebra/framework/queues/base.py` — `QueueConfig` class docstring names every queue backend; rendered in `docs/reference/queues.md` (ch10 / t03).
- `[ ]` `README.md` — Driver list marks Oracle and SQL Server as pending native drivers (ch11 / t01).
- `[ ]` `docs/usage/databases.md` — Engine table status, DuckDB `duckdb` requirement and free-threaded caveat, `memory://` scope (ch11 / t01).
- `[ ]` `docs/usage/extensions.md` — Dialect engine claims and `InterfaceError` behavior (ch11 / t01).
- `[ ]` `docs/reference/db.md` — Adapter descriptions (ch11 / t02).
- `[ ]` `docs/reference/index.md` — Adapter summary line (ch11 / t02).
- `[ ]` `docs/usage/analytics-and-cdc.md` — Oracle narrative without stand-in execution (ch11 / t02).
- `[ ]` `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Adapter table, codec inventory, install hints, conformance outcomes (ch11 / t03).
- `[ ]` `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — "Database driver conformance" section outcomes (ch11 / t03); GIL-only DuckDB group, `pytest.importorskip("duckdb")` rule, and `resp3_server` fixture in the other sections (ch13 / t02).
- `[ ]` `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — Tenancy text without SQLite stand-in (ch11 / t04).
- `[ ]` `.agents/bundles/product/product.md` — `vertebra-db` component row (ch11 / t04).
- `[ ]` `.agents/bundles/knowledge/decisions/adr-002-two-tier-shm-database-and-arrow-data-plane.md` — Consequences without TNS/TDS framing (ch11 / t04).
- `[ ]` `docs/usage/queues-and-realtime.md` — Backend requirements and install packages (ch12 / t01).
- `[ ]` `docs/reference/queues.md` — Storage Backends row and autodoc blocks including Spanner (ch12 / t01).
- `[ ]` `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — Backend table and executor/client rules (ch12 / t01).
- `[ ]` `AGENTS.md` — `make install` entry with `--group duckdb` (ch13 / t01).
- `[ ]` `.agents/bundles/knowledge/workflow.md` — Install, test, coverage, and upgrade commands with the GIL-only `duckdb` group (ch13 / t01).
- `[ ]` `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` — `bench` includes the `duckdb` group; free-threaded runs exclude it (ch13 / t01).
- `[ ]` `.agents/bundles/product/tech-stack.md` — `vertebra-db` dependencies without `sha2` / `rmp-serde`; `duckdb` group (ch13 / t02).
- `[ ]` `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — DB-API `InterfaceError`; Oracle and MSSQL sessions raise `MissingDependencyError` (ch13 / t03).
- `[ ]` `.agents/bundles/knowledge/domains/index.md` — Oracle and MSSQL drivers and `OracleEmbeddingPushdown` marked pending (ch13 / t03).
- `[ ]` `docs/index.md` — Engine list without a count; Oracle and SQL Server native drivers not yet published (ch13 / t03).
- `[ ]` `docs/usage/embeddings-and-vectors.md` — `OracleEmbeddingPushdown` requires the pending native Oracle driver (ch13 / t04).
- `[ ]` `docs/usage/index.md` — Embeddings entry states the Oracle pushdown requirement (ch13 / t04).
- `[ ]` `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md` — Oracle pushdown requirement with refreshed provenance (ch13 / t04).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Chapter / Task | Verification Strategy | Canonical Test / Doc Gate |
| --- | --- | --- | --- |
| R1 | ch01 / t01, t02 | behavior_tdd | `src/py/tests/unit/db/test_enterprise_olap_drivers.py` |
| R2 | ch01 / t02 | behavior_tdd | `src/py/tests/unit/db/test_enterprise_olap_drivers.py`, `src/py/tests/unit/db/test_native_db_drivers.py` |
| R3 | ch02 / t01, t04 | static_validation (t01), behavior_tdd (t04) | `uv lock --check`, `make -n py-test` group checks; `src/py/tests/unit/db/test_enterprise_olap_drivers.py`, `src/py/tests/unit/storage/test_gcs_token_refresh.py` |
| R4 | ch02 / t02 | behavior_tdd | `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| R5 | ch02 / t03 | behavior_tdd | `src/py/tests/unit/framework/test_distributed_queues.py` |
| R6 | ch03 / t01 | behavior_tdd | `src/py/tests/unit/db/test_native_db_drivers.py` |
| R7a | ch03 / t02, t03 | static_validation | `src/py/tests/unit/db/test_redis_valkey_driver.py`, `src/py/tests/unit/extensions/test_alchemy_cache_backend.py` |
| R7b | ch04 / t01 | behavior_tdd | `src/py/tests/unit/db/test_native_db_drivers.py` |
| R7c | ch04 / t02 | behavior_tdd | `src/py/tests/unit/db/test_redis_valkey_driver.py` |
| R8.1 (MSSQL Rust) | ch05 / t03 | static_validation | `make rs-test` |
| R8.2 (Oracle Rust) | ch06 / t01 | static_validation | `make rs-test`, `src/py/tests/unit/db/test_enterprise_olap_drivers.py -k vector_oson` |
| R8.3 (PostgreSQL and DuckDB pools) | ch07 / t01 | static_validation | `make rs-test` |
| R8.4 (SQLite pool, `rmp-serde`) | ch07 / t02 | static_validation | `make rs-test`, `cargo check --workspace --locked` |
| R8.5 (MySQL pool, `OracleWalletMaterial`) | ch07 / t03 | static_validation | `make rs-test` |
| R8.6 (`mysql` module, `sha2`, crate docs) | ch07 / t04 | static_validation | `make rs-test`, `make lint-all`, `cargo doc -p vertebra-db --no-deps` |
| R9.1 (MSSQL Python, binding) | ch05 / t01, t02 | static_validation | `mypy.stubtest vertebra._vertebra`, `src/py/tests/unit/db/test_enterprise_olap_drivers.py` |
| R9.2 (Oracle, Redis, DuckDB Python) | ch06 / t02, t03 | static_validation | `src/py/tests/unit/db/test_enterprise_olap_drivers.py`, `src/py/tests/unit/db/test_redis_valkey_resp3.py` |
| R10 | ch08 / t01, t02, t03, t04 | behavior_tdd | `src/py/tests/unit/framework/test_typing.py`, `src/py/tests/unit/build/test_packaging_and_pgo.py` |
| R11 | ch08 / t04 | behavior_tdd | `src/py/tests/unit/build/test_packaging_and_pgo.py` |
| R12 | ch09 / t01 | behavior_tdd | `src/py/tests/integration/db/test_driver_conformance.py` |
| R13 | ch11 / t01, t02, t03, t04 | documentation_validation | `make docs`, OKF frontmatter/footnote checks |
| R14 | ch10 / t01 | behavior_tdd | `src/py/tests/unit/framework/test_distributed_queues.py` |
| R15 | ch10 / t02 | behavior_tdd | `src/py/tests/unit/framework/test_distributed_queues.py` |
| R16 | ch10 / t03 | behavior_tdd | `src/py/tests/unit/db/test_spanner_queue_and_cdc.py` |
| R17 | ch12 / t01 | documentation_validation | `make docs`, `make review-check`, OKF footnote check |
| R18.1 | ch13 / t01 | documentation_validation | `make review-check`, OKF footnote check |
| R18.2 | ch13 / t02 | documentation_validation | `make review-check`, OKF footnote check |
| R18.3 | ch13 / t03 | documentation_validation | `make docs`, `make review-check`, OKF footnote check |
| R18.4 | ch13 / t04 | documentation_validation | `make docs`, `make review-check`, OKF footnote check |

## 5. Implementation Plan

- [x] [d7a629b1] ch01-oracle-mssql-fail-loud — Oracle and MSSQL drivers raise `MissingDependencyError` with no SQLite path; `AsyncMSSQLDriver` forwards `dialer` ([spec](chapters/ch01-oracle-mssql-fail-loud/spec.md))
- [x] [af67511d] ch02-duckdb-sqlalchemy-queue-fail-loud — GIL-only `duckdb` group, DuckDB requires `duckdb`, SQLAlchemy raises `InterfaceError`, Oracle queue requires an executor ([spec](chapters/ch02-duckdb-sqlalchemy-queue-fail-loud/spec.md))
- [x] [ea07cb21] ch03-remove-memory-fakes — PostgreSQL and MySQL drivers reject `memory://`; Redis and Valkey consumer tests move onto the `resp3_server` loopback fixture ([spec](chapters/ch03-remove-memory-fakes/spec.md))
- [ ] ch04-redis-valkey-memory-removal — `RedisConfig` drops the `memory` scheme; Redis and Valkey drivers and store factories reject `memory://` ([spec](chapters/ch04-redis-valkey-memory-removal/spec.md))
- [ ] ch05-mssql-codec-cleanup — MSSQL Python batch wrappers, the `encode_tds_sql_batch` binding and stub, Rust TDS framing, `MSSQL_DIALECT_TAG`, and `MssqlPool` removed; the placeholder rewriter stays ([spec](chapters/ch05-mssql-codec-cleanup/spec.md))
- [ ] ch06-oracle-codec-cleanup — Rust TNS framing, `TnsPacketType`, `ORACLE_DIALECT_TAG`, and `OraclePool` removed; Oracle TNS and Redis hash-slot probes for unexported natives deleted; Oracle and DuckDB docs describe codecs ([spec](chapters/ch06-oracle-codec-cleanup/spec.md))
- [ ] ch07-vertebra-db-pools-and-mysql — PostgreSQL, DuckDB, SQLite, and MySQL pools and dialect tags, the `OracleWalletMaterial` struct, the `mysql` module, and the `sha2` / `rmp-serde` dependencies removed; crate docs describe codecs ([spec](chapters/ch07-vertebra-db-pools-and-mysql/spec.md))
- [ ] ch08-install-hints-and-extras-gate — `MissingDependencyError` hints default to `vertebra-core[<extra>]`; undeclared extras and sentence-valued package names are replaced behind an AST extras gate ([spec](chapters/ch08-install-hints-and-extras-gate/spec.md))
- [ ] ch09-conformance-outcomes — Conformance skips a missing native module, fails under Docker only for non-pending engines, and applies strict `xfail` only when a pending engine's module imports ([spec](chapters/ch09-conformance-outcomes/spec.md))
- [ ] ch10-queue-backend-fail-loud — PostgreSQL, Redis/Valkey, and Spanner queue backends require a live executor or client ([spec](chapters/ch10-queue-backend-fail-loud/spec.md))
- [ ] ch11-docs-and-okf — Database user docs and OKF knowledge match the fail-loud baseline ([spec](chapters/ch11-docs-and-okf/spec.md))
- [ ] ch12-queue-docs-and-knowledge — Queue docs and OKF knowledge state executor-or-raise and client-or-raise rules ([spec](chapters/ch12-queue-docs-and-knowledge/spec.md))
- [ ] ch13-agent-knowledge-and-embeddings-docs — Agent instructions, tech stack, test matrix, dialect knowledge, landing pages, and Oracle embedding docs match the baseline ([spec](chapters/ch13-agent-knowledge-and-embeddings-docs/spec.md))
- **Chapter / Phase Recovery Checkpoint**: `make lint && make py-test` (ch05, ch06, and ch07 add `make rs-test`; ch11, ch12, and ch13 add `make docs`)

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `fd571e3a` — plan authored; `0/13` chapters and `0/38` tasks completed.
- **Active Chapter**: `ch01-oracle-mssql-fail-loud` (`0/2` tasks completed).
- **Remaining Plan**: `ch01` through `ch13`.
- **Documentation & OKF Sync**: `[0/33 files documented]`.
- **Active Invariants & Triggers**: registry import stays free of driver imports; `_vertebra.pyi` changes ship with binding changes; `MemoryQueueBackend`, object-storage `memory://`, and SQLite `:memory:` backends are untouched; free-threaded interpreters never install `duckdb`.
- **Breaking removals for the changelog**: Oracle driver `set_session_attrs` and `last_piggyback_frame`; MSSQL driver `last_rewritten_sql` and `last_tds_batch`; `MSSQLConfig.encode_sql_batch`, `MSSQLCore.encode_tds_sql_batch`, and the `encode_tds_sql_batch` binding and package export; the `AsyncMSSQLDriver` `dialer` parameter now takes `SyncCloudSQLDialer`; `AsyncPostgreSQLDriver` no longer probes `asyncpg` and requires `psycopg_pool`; the Spanner queue backend `driver=` parameter; `memory://` engine URLs for PostgreSQL, MySQL, Redis, and Valkey drivers and the Redis/Valkey store factories; the `RedisConfig` `memory` scheme; the in-memory Oracle, PostgreSQL, and Spanner queue executors and Redis/Valkey stream client, so those queue backends need `executor=` or `client=`.

[^module-loader]: `src/py/vertebra/utils/module_loader.py::ensure_module` — Optional module loader raising MissingDependencyError.
[^missing-dep]: `src/py/vertebra/_typing.py::MissingDependencyError` — Missing dependency error and install hint.
[^oracle-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Oracle driver backed by sqlite3.
[^mssql-driver]: `src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver` — MSSQL driver backed by sqlite3.
[^duckdb-driver]: `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver` — DuckDB driver with sqlite3 fallback.
[^dep-groups]: `pyproject.toml::dependency-groups` — Dependency groups; `duckdb` only in `bench`.
[^sqla-dbapi]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection` — SQLAlchemy DB-API connection with SQLite parse fallback.
[^oracle-queue]: `src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend` — Oracle queue backend with in-memory executor fallback.
[^pg-queue]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQL queue backend with in-memory executor fallback.
[^redis-queue]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — Redis and Valkey queue backend with in-memory stream client fallback.
[^spanner-queue]: `src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend` — Spanner queue backend with in-memory executor fallback.
[^redis-config]: `src/py/vertebra/db/adapters/redis/config.py::RedisConfig` — Redis config accepting the memory scheme.
[^litestar-store]: `src/py/vertebra/extensions/litestar/store.py::MemoryStore` — Litestar store with from_valkey and from_redis factories.
[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — vertebra-db re-exports of pools and codecs.
[^db-bindings]: `src/rs/vertebra-py/src/db.rs::register_db_bindings` — Python codec bindings on vertebra._vertebra.
[^conformance]: `src/py/tests/integration/db/test_driver_conformance.py::_resolve_live_config` — Conformance live config resolution.
[^packaging-test]: `src/py/tests/unit/build/test_packaging_and_pgo.py` — Packaging extras assertions.
