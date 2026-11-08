---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch25-dsn-bridge-lexer-and-permanent-drivers
title: "Chapter 25: DSN/libpq Config Bridge, Lexer Bind Compiler, and Permanent Native Drivers"
description: Implement the pure-Python PostgreSQL URI/libpq option bridge, single-pass lexical bind compiler, and permanent SyncPostgreSQLDriver and AsyncPostgreSQLDriver facades before public cutover.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch24-change-source-and-checkpoints
replan_trigger: null
current_task: "native-postgres-driver:ch25:t01"
plan_revision: 9
plan_commit: b0de4dcb
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T05:00:00Z"
generated:
  by: flow-revise
  at: "2026-11-07T05:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T05:00:00Z"
    commit: dd432f18
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Native connection constructor kwargs, DSN precedence, lexical bind rules, and permanent driver facades
  - id: pg-config-py
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: Existing PostgreSQLConfig fields, pooler modes, and cloud dialer options
  - id: pg-core-py
    resource: src/py/vertebra/db/adapters/postgres/core.py::PostgreSQLCore
    title: Adapter-owned PostgreSQLCore helpers
  - id: pg-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Canonical public PostgreSQL driver barrel preserved until Chapter 27 activation
tags:
  - postgres
  - configuration
  - lexer
  - driver
  - cutover
---

# Chapter 25: DSN/libpq Config Bridge, Lexer Bind Compiler, and Permanent Native Drivers

<!-- truth: start -->
- **Single Adapter DSN/libpq Bridge**: `options.py` and `PostgreSQLConfig.{resolve_sync_options, resolve_async_options}` parse `postgresql://` URIs (strict UTF-8 percent-decoding preserving literal `+`) and libpq `key=value` DSNs into primitive `vertebra._postgres` kwargs with documented `PG*` environment and `sslmode="prefer"` adapter defaults, lazy PEM loading, secret redaction, and zero cross-extension `GcpTokenCache` `Arc` sharing [^npd-contracts] [^pg-config-py].
- **Lexer-Aware Bind Lowering**: `binds.py` and `PostgreSQLCore.prepare_native_query` pass native `$N` SQL through untouched and lower `%s` / `%(name)s` to `$N` via a single-pass state machine that ignores single quotes, `E'...'` strings, `""` identifiers, `$$`/`$tag$` dollar quotes, and `--`/`/*...*/` comments while preserving `::` casts, `?`/`?|`/`?&` JSONB operators, and `foo$1` identifiers [^npd-contracts] [^pg-core-py].
- **Permanent Staged Driver Pair**: `_sync.py::SyncPostgreSQLDriver` and `_async.py::AsyncPostgreSQLDriver` own native `SyncPostgresPool` / `AsyncPostgresPool` leases, `ContextVar` thread/task-pinned transactions and savepoints, command-only `timeout` forwarding, and generation-fenced active-operation `cancel()` registries while `driver.py` stays untouched until `ch27:t01` [^npd-contracts] [^pg-driver-py].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Replacing `psycopg` and `asyncpg` requires an adapter-owned URI and libpq keyword DSN parser, a SQL-lexer bind compiler that lowers legacy pyformat `%s` / `%(name)s` calls without corrupting dollar-quoted PL/pgSQL bodies or JSONB `?` operators, and complete native `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` implementations staged cleanly before atomic barrel activation [^npd-contracts] [^pg-config-py] [^pg-driver-py]. Chapter 25 delivers `options.py` + `PostgreSQLConfig` option resolution, `binds.py` + `PostgreSQLCore.prepare_native_query`, `_sync.py::SyncPostgreSQLDriver`, and `_async.py::AsyncPostgreSQLDriver` while keeping legacy `driver.py` exports green prior to Chapter 27.

### 1.2 Non-Goals

- SQLSpec `ParameterStyle.NUMERIC` copy helpers and SQLAlchemy `numeric_dollar` compiler profiles (`ch26:t01`), `pipeline()` context factories (`ch26:t02`), Arrow edge-case completion (`ch26:t03`), and dedicated event hooks (`ch26:t04`).
- Switching `driver.py` / `__init__.py` public exports or deleting legacy `_LegacySyncPostgreSQLDriver` / `_LegacyAsyncPostgreSQLDriver` bodies (`ch27:t01`–`t04`).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and [contracts.md](../../contracts.md). Skills: [python-framework](../../../../../skills/python-framework/SKILL.md) and [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md). Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md) and [adr-004-optional-driver-wheels-and-shm-ownership.md](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R2.5 (`ch25:t01`)**: Implement `PostgresConnectOptions` and URI/libpq keyword parser in `options.py`, extend `PostgreSQLConfig` in `config.py` with nullable TLS/timeout/keepalive overrides, `cloud_auth_options`, fixed `to_cloud_dialer_config` IP forwarding, strict `derive_direct_config()` pooler validation, and `resolve_sync_options` / `resolve_async_options`.
- **R2.6 & R24.1 (`ch25:t02`)**: Implement single-pass lexical bind compiler in `binds.py` and `PostgreSQLCore.prepare_native_query` in `core.py` handling `%s` / `%(name)s` -> `$N`, native `$N` passthrough, `foo$1` boundary preservation, ordinary backslash-quote rejection in emulation mode, and bounded metadata caching.
- **R6, R13, R18, R24.2 (`ch25:t03`)**: Implement permanent `SyncPostgreSQLDriver` in `src/py/vertebra/db/adapters/postgres/_sync.py` with `SyncPostgresPool` validation, `ContextVar` thread-pinned transactions/savepoints, `query_multi`, command `timeout` forwarding, thin Arrow forwarders, and active-operation `cancel()` registry.
- **R7, R13, R18, R24.3 (`ch25:t04`)**: Implement permanent `AsyncPostgreSQLDriver` in `src/py/vertebra/db/adapters/postgres/_async.py` directly over `AsyncPostgresPool` (zero `SyncPostgreSQLDriver` coupling) with task-pinned transactions/savepoints, `query_multi`, command `timeout` forwarding, thin async Arrow forwarders, and active-operation `cancel()` registry.

### 2.2 Concrete Type & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/options.py` & `config.py`:
  - `class PostgresConnectOptions(msgspec.Struct, frozen=True, gc=False, kw_only=True): ...` (secret-redacting `__repr__`, `to_native_client_kwargs() -> dict[str, Any]`, `to_native_pool_kwargs() -> dict[str, Any]`)
  - `def parse_postgres_dsn(dsn: str) -> ParsedPostgresDsn`
  - `PostgreSQLConfig.resolve_sync_options(self, *, include_pool_options: bool = True) -> dict[str, Any]`
  - `PostgreSQLConfig.resolve_async_options(self, *, include_pool_options: bool = True) -> dict[str, Any]`
  - `PostgreSQLConfig.derive_direct_config(self) -> PostgreSQLConfig`
- `src/py/vertebra/db/adapters/postgres/binds.py` & `core.py`:
  - `def compile_postgres_binds(sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> tuple[str, tuple[Any, ...]]`
  - `PostgreSQLCore.prepare_native_query(sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> tuple[str, tuple[PostgresValue | PgParameter, ...]]`
- `src/py/vertebra/db/adapters/postgres/_sync.py` & `_async.py`:
  - `class SyncPostgreSQLDriver(SyncDatabaseDriver[PostgreSQLConfig]):` (`connect`, `close`, `begin`, `commit`, `rollback`, `transaction`, `cancel`, `execute`, `execute_many`, `execute_with_description`, `select`, `query_multi(self, sql: str, *, timeout: float | None = None) -> list[PostgresQueryResult]`, `select_to_arrow`, `stream_arrow_batches`, `load_from_arrow`)
  - `class AsyncPostgreSQLDriver(AsyncDatabaseDriver[PostgreSQLConfig]):` (async counterparts of the complete surface above with zero sync fallback)

### 2.3 Caller -> Seam -> Collaborator Map

- `SyncPostgreSQLDriver` / `AsyncPostgreSQLDriver` -> `PostgreSQLConfig.{resolve_sync_options, resolve_async_options}` -> `options.parse_postgres_dsn` -> `vertebra._postgres.{SyncPostgresPool, AsyncPostgresPool}`.
- Driver query methods -> `PostgreSQLCore.prepare_native_query` -> `binds.compile_postgres_binds` + `types` value validation -> native `client.query(..., timeout=timeout)`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Condition | Required Behavior |
| --- | --- |
| URI contains literal `+` (`p+w`), `%25`, invalid `%FF` UTF-8, or unknown DSN option `target_session_attrs=read-write` | Literal `+` stays `+`; invalid UTF-8 / percent escape raises `ValueError`; unknown/multi-host option raises `NotSupportedError`; secrets never appear in error strings or `repr()`. |
| Emulated `%s` SQL contains `'it\'s'` backslash-quote vs `E'it\'s'`, `$$ %s $$`, `-- %s`, `/* /* %(x)s */ */`, `a::text`, `doc ? 'k'`, or `foo$1` | `'it\'s'` raises `ProgrammingError` before I/O; `E'it\'s'` and quoted/commented/JSONB/`foo$1` tokens stay intact; numeric-only SQL (`SELECT $1`) passes unchanged without scanning backslash quotes. |
| `driver.cancel()` invoked with `0`, `1` (parent or child worker), or `>= 2` active operations | `0` is a no-op; `1` claims and generation-fences cancel on that exact client and retires its socket; `>= 2` raises `InterfaceError` with zero `CancelRequest` packets; other drivers sharing the pool are untouched. |
| Inherited `ContextVar` transaction accessed from child thread or spawned `asyncio.Task` | Owner `(thread_id, task_id)` check rejects cross-context socket sharing with `InterfaceError`. |

### 2.5 Mandatory Alternatives Comparison

| Decision | Chosen Design | Rejected Alternative & Why |
| --- | --- | --- |
| Legacy bind lowering | Single-pass SQL lexical state machine with numeric passthrough | Regex replacement (`re.sub`), which corrupts `%s`/`?`/`$1` inside dollar-quoted functions, comments, and JSONB operators. |
| Pre-activation driver staging | Final `_sync.py` and `_async.py` modules imported directly by new tests until `ch27:t01` | Editing `driver.py` piecemeal while legacy `AsyncPostgreSQLDriver.__init__` still instantiates legacy `SyncPostgreSQLDriver`, breaking Mypy and baseline tests. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch25]` (consolidated in `ch30:t01`–`ch30:t04`; Google-style docstrings on `options.py`, `binds.py`, `_sync.py`, and `_async.py` document DSN precedence, lexical rules, and per-command `timeout` boundaries).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R2 (R2.5) | [t01](tasks/t01-dsn-and-libpq-config-bridge.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_options.py` |
| R2, R24 (R2.6, R24.1) | [t02](tasks/t02-lexer-aware-bind-compiler.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_binds.py` |
| R6, R13, R18, R24 (R24.2) | [t03](tasks/t03-permanent-sync-postgres-driver.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_native_adapters.py` |
| R7, R13, R18, R24 (R24.3) | [t04](tasks/t04-permanent-async-postgres-driver.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_native_adapters.py` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch25:t01 — Bridge PostgreSQL URL and libpq configuration into native options (tasks/t01-dsn-and-libpq-config-bridge.md)
- [ ] native-postgres-driver:ch25:t02 — Lower PostgreSQL legacy binds with a lexer-aware adapter compiler (tasks/t02-lexer-aware-bind-compiler.md)
- [ ] native-postgres-driver:ch25:t03 — Implement the permanent synchronous native PostgreSQL driver before activation (tasks/t03-permanent-sync-postgres-driver.md)
- [ ] native-postgres-driver:ch25:t04 — Implement the permanent asynchronous native PostgreSQL driver before activation (tasks/t04-permanent-async-postgres-driver.md)

**Chapter Recovery Checkpoint:** `make dev && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_options.py src/py/tests/unit/db/test_postgres_binds.py src/py/tests/unit/db/test_postgres_native_adapters.py src/py/tests/unit/db/test_postgres_driver.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ dd432f18`; chapter `state: planned`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending).
- **Downstream Dependents**: Unblocks `ch26-compiler-pipeline-arrow-and-event-hooks` and `ch27-atomic-cutover-and-legacy-removal`.

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native connection constructor kwargs, DSN precedence, lexical bind rules, and permanent driver facades.
[^pg-config-py]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — Existing PostgreSQLConfig fields, pooler modes, and cloud dialer options.
[^pg-core-py]: `src/py/vertebra/db/adapters/postgres/core.py::PostgreSQLCore` — Adapter-owned PostgreSQLCore helpers.
[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Canonical public PostgreSQL driver barrel preserved until Chapter 27 activation.
