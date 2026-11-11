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
plan_revision: 6
plan_commit: 043d8444
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-10T14:45:00Z"
generated:
  by: flow-revise
  at: "2026-11-10T14:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: 043d8444
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Native connection constructor kwargs, DSN precedence, lexical bind rules, and permanent driver facades
  - id: pg-config-py
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: Existing PostgreSQLConfig fields, from_url, pool_key, pooler modes, and cloud dialer options
  - id: pg-core-py
    resource: src/py/vertebra/db/adapters/postgres/core.py::PostgreSQLCore
    title: Adapter-owned PostgreSQLCore helpers
  - id: pg-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Canonical public PostgreSQL driver barrel and static helpers preserved until Chapter 27 activation
tags:
  - postgres
  - configuration
  - lexer
  - driver
  - cutover
---

# Chapter 25: DSN/libpq Config Bridge, Lexer Bind Compiler, and Permanent Native Drivers

<!-- truth: start -->
- **Single Adapter DSN/libpq Bridge**: `options.py` and `PostgreSQLConfig.{from_url, pool_key, resolve_sync_options, resolve_async_options}` parse `postgresql://`/`postgres://` URIs (`+`-preserving strict UTF-8 percent-decoding), libpq `key=value` DSNs, and `-c key=val`/`-ckey=val`/`\ `-escaped `options` into primitive `vertebra._postgres` kwargs with documented `PG*` defaults, synchronous `memory://` (`ImproperConfigurationError`) and `sslmode="allow"` (`ValueError`) rejection, secret-free cache keys/`repr()`, and zero cross-extension `Arc` sharing [^npd-contracts] [^pg-config-py].
- **Lexer-Aware Bind Lowering**: `binds.py` and `PostgreSQLCore.prepare_native_query` pass native `$N` SQL through untouched and lower `%s` / `%(name)s` to `$N` via a single-pass class-encapsulated state machine covering single quotes, `E'...'` strings, `""` identifiers, `$$`/`$tag$` dollar quotes, `--` (including EOF without `\n`) and nested `/*...*/` comments, `%%s` -> literal `%s` escapes, `::` casts, `?`/`?|`/`?&` JSONB operators, and `foo$1` identifiers [^npd-contracts] [^pg-core-py].
- **Permanent Staged Driver Pair**: `_sync.py::SyncPostgreSQLDriver` and `_async.py::AsyncPostgreSQLDriver` validate `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")`, synchronous `memory://` and external `pool=` contracts, `dialer` properties, legacy static COPY/JSONB helpers, `ContextVar` thread/task-pinned transactions, command-only `timeout` forwarding, and active-operation `cancel()` registries while `driver.py` stays untouched until `ch27:t01` [^npd-contracts] [^pg-driver-py].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Replacing `psycopg` and `asyncpg` requires an adapter-owned URI and libpq keyword DSN parser aligned with `PostgreSQLConfig.from_url` and `DatabaseRegistry` `pool_key()` caching, a SQL-lexer bind compiler that lowers legacy pyformat `%s` / `%(name)s` calls without corrupting dollar-quoted PL/pgSQL bodies, `%%s` literals, EOF line comments, or JSONB `?` operators, and complete native `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` implementations staged cleanly before atomic barrel activation [^npd-contracts] [^pg-config-py] [^pg-driver-py]. Chapter 25 delivers `options.py` + `PostgreSQLConfig` option/pool-key resolution, `binds.py` + `PostgreSQLCore.prepare_native_query`, `_sync.py::SyncPostgreSQLDriver`, and `_async.py::AsyncPostgreSQLDriver` while keeping legacy `driver.py` exports green prior to Chapter 27.

### 1.2 Non-Goals

- SQLSpec `ParameterStyle.NUMERIC` copy helpers and SQLAlchemy `numeric_dollar` compiler profiles (`ch26:t01`), `pipeline()` context factories (`ch26:t02`), Arrow edge-case completion (`ch26:t03`), and dedicated event hooks (`ch26:t04`).
- Switching `driver.py` / `__init__.py` public exports or deleting legacy `_LegacySyncPostgreSQLDriver` / `_LegacyAsyncPostgreSQLDriver` bodies (`ch27:t01`–`t04`).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and [contracts.md](../../contracts.md). Skills: [python-framework](../../../../../skills/python-framework/SKILL.md) and [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md). Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md) and [adr-004-optional-driver-wheels-and-shm-ownership.md](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R2.5 (`ch25:t01`)**: Implement `PostgresConnectOptions` (`frozen=True, kw_only=True`, without `gc=False`, storing `session_gucs`/`cloud_auth_options`/`iam_auth_options` as `tuple[tuple[str, str], ...]`) and URI/libpq keyword parser in `options.py`; extend `PostgreSQLConfig` in `config.py` with `+`-preserving `from_url`, full resolved secret-safe `pool_key()`, synchronous `memory://` (`ImproperConfigurationError`), unsupported scheme, and `sslmode="allow"` (`ValueError`) rejection, `-c key=val`/`-ckey=val`/`\ ` libpq `options` parsing with reserved-GUC rejection, fixed `to_cloud_dialer_config` IP forwarding, strict `derive_direct_config()`, and `resolve_sync_options` / `resolve_async_options`.
- **R2.6 & R24.1 (`ch25:t02`)**: Implement class-organized single-pass lexical bind compiler in `binds.py` and `PostgreSQLCore.prepare_native_query` in `core.py` handling `%s` / `%(name)s` -> `$N`, `%%s` -> literal `%s` (even `%` runs never consume parameters), `--` EOF line-comment termination without trailing `\n`, native `$N` passthrough, `foo$1` boundary preservation, ordinary backslash-quote rejection in emulation mode, and bounded metadata caching.
- **R6, R13, R18, R24.2 (`ch25:t03`)**: Implement permanent `SyncPostgreSQLDriver` in `src/py/vertebra/db/adapters/postgres/_sync.py` with `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")`, synchronous case-insensitive `memory://` and invalid external `pool=` (`ImproperConfigurationError`) rejection, read-only `dialer`/`pool` properties, preserved static helpers (`strip_jsonb_header`, `encode_jsonb`, `encode_pgcopy_binary_rows`, `parse_pgcopy_binary_rows`), `ContextVar` thread-pinned transactions/savepoints, `query_multi`, command `timeout` forwarding, Arrow forwarders, and active-operation `cancel()` registry.
- **R7, R13, R18, R24.3 (`ch25:t04`)**: Implement permanent `AsyncPostgreSQLDriver` in `src/py/vertebra/db/adapters/postgres/_async.py` directly over `AsyncPostgresPool` (zero `SyncPostgreSQLDriver` coupling) with identical `ensure_module` metadata, synchronous case-insensitive `memory://` and invalid `pool=` rejection, read-only `dialer`/`pool` properties, preserved static helpers (`strip_jsonb_header`, `encode_jsonb`), task-pinned transactions/savepoints, `query_multi`, command `timeout` forwarding, async Arrow forwarders, and active-operation `cancel()` registry.

### 2.2 Concrete Type & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/options.py` & `config.py`:
  - `class PostgresConnectOptions(msgspec.Struct, frozen=True, kw_only=True): ...` (`session_gucs`, `cloud_auth_options`, and `iam_auth_options` typed as `tuple[tuple[str, str], ...] = ()`, converted to `dict[str, str]` only inside `to_native_client_kwargs() -> dict[str, Any]` and `to_native_pool_kwargs() -> dict[str, Any]`; secret-redacting `__repr__`)
  - `def parse_postgres_dsn(dsn: str) -> ParsedPostgresDsn`
  - `PostgreSQLConfig.from_url(cls, url: str, **overrides: Any) -> Self` and `PostgreSQLConfig.pool_key(self) -> tuple[Any, ...]`
  - `PostgreSQLConfig.resolve_sync_options(self, *, include_pool_options: bool = True) -> dict[str, Any]`
  - `PostgreSQLConfig.resolve_async_options(self, *, include_pool_options: bool = True) -> dict[str, Any]`
  - `PostgreSQLConfig.derive_direct_config(self) -> PostgreSQLConfig`
- `src/py/vertebra/db/adapters/postgres/binds.py` & `core.py`:
  - `def compile_postgres_binds(sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> tuple[str, tuple[Any, ...]]` (delegating to structured `PostgresBindCompiler` methods; zero single-caller module-level `_...` functions)
  - `PostgreSQLCore.prepare_native_query(sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> tuple[str, tuple[PostgresValue | PgParameter, ...]]`
- `src/py/vertebra/db/adapters/postgres/_sync.py` & `_async.py`:
  - `class SyncPostgreSQLDriver(SyncDatabaseDriver[PostgreSQLConfig]):` (`dialer`, `pool`, `strip_jsonb_header`, `encode_jsonb`, `encode_pgcopy_binary_rows`, `parse_pgcopy_binary_rows`, `connect`, `close`, `begin`, `commit`, `rollback`, `transaction`, `cancel`, `execute`, `execute_many`, `execute_with_description`, `select`, `query_multi(self, sql: str, *, timeout: float | None = None) -> list[PostgresQueryResult]`, `select_to_arrow`, `stream_arrow_batches`, `load_from_arrow`)
  - `class AsyncPostgreSQLDriver(AsyncDatabaseDriver[PostgreSQLConfig]):` (`dialer`, `pool`, `strip_jsonb_header`, `encode_jsonb`, and async counterparts of the driver execution/lifecycle surface with zero sync fallback)

### 2.3 Caller -> Seam -> Collaborator Map

- `DatabaseRegistry` / `SyncPostgreSQLDriver` / `AsyncPostgreSQLDriver` -> `PostgreSQLConfig.{from_url, pool_key, resolve_sync_options, resolve_async_options}` -> `options.parse_postgres_dsn` -> `vertebra._postgres.{SyncPostgresPool, AsyncPostgresPool}`.
- Driver query methods -> `PostgreSQLCore.prepare_native_query` -> `binds.compile_postgres_binds` + `types` value validation -> native `client.query(..., timeout=timeout)`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Condition | Required Behavior |
| --- | --- |
| `memory://` (any case), unsupported URL scheme (`mysql://`), `sslmode="allow"` (DSN, typed field, or `PGSSLMODE`), or reserved GUC in `options` (`-c user=x`, `-c client_encoding=LATIN1`) | `memory://` raises `ImproperConfigurationError(match="memory://")` synchronously in `parse_postgres_dsn` and both driver `__init__` methods; unsupported scheme raises `ImproperConfigurationError`/`ValueError`; `sslmode="allow"` raises `ValueError`; reserved GUCs (`user`, `database`, `options`, `replication`, non-`UTF8` `client_encoding`) fail closed while `-c key=val`, `-ckey=val`, and `\ `-escaped values parse cleanly. |
| URI contains literal `+` (`p+w` in `from_url` or `dsn`), `%25`, invalid `%FF` UTF-8, unknown DSN option `target_session_attrs=read-write`, or distinct TLS/GUC/IAM configs in `pool_key()` | Literal `+` stays `+`; invalid UTF-8 / percent escape raises `ValueError`; unknown/multi-host option raises `NotSupportedError`; `pool_key()` distinguishes resolved socket/TLS/GUC/IAM/credential/preparation configs via SHA-256 digests without exposing plaintext secrets in `repr()`. |
| Emulated `%s` SQL contains `%%s`, `'it\'s'` vs `E'it\'s'`, `$$ %s $$`, `-- %s` at EOF without `\n`, `/* /* %(x)s */ */`, `a::text`, `doc ? 'k'`, or `foo$1` | `%%s` lowers to literal `%s` consuming zero parameters (`%%%s` -> `%$1`); `-- %s` at EOF terminates cleanly without consuming parameters; `'it\'s'` raises `ProgrammingError` before I/O; `E'it\'s'`, quoted/commented/JSONB, and `foo$1` stay intact; numeric-only SQL passes unchanged. |
| Missing `vertebra._postgres` wheel, invalid external `pool=` missing `validate_configuration`/`acquire`, inherited `ContextVar` across threads/tasks, or `driver.cancel()` with `0`/`1`/`>= 2` active ops | Missing wheel raises `MissingDependencyError` with `package == "vertebra-core[postgres]"` in `str(exc)`; invalid external `pool=` raises `ImproperConfigurationError`; cross-thread/task `ContextVar` raises `InterfaceError`; `cancel()` no-ops on `0`, retires the single active session on `1`, and raises `InterfaceError` with zero packets on `>= 2`. |

### 2.5 Mandatory Alternatives Comparison

| Decision | Chosen Design | Rejected Alternative & Why |
| --- | --- | --- |
| `PostgresConnectOptions` container strategy | `msgspec.Struct(frozen=True, kw_only=True)` storing GUC/auth pairs as `tuple[tuple[str, str], ...]` and omitting `gc=False` | Combining `gc=False` with `dict`/`Mapping` fields, which risks GC cycle retention and unhashable config snapshots. |
| Legacy bind lowering | Single-pass class-encapsulated SQL lexical state machine with numeric passthrough | Regex replacement (`re.sub`), which corrupts `%s`/`%%s`/`?`/`$1` inside dollar-quoted functions, EOF line comments, and JSONB operators. |
| Pre-activation driver staging | Final `_sync.py` and `_async.py` modules imported directly by new tests until `ch27:t01` | Editing `driver.py` piecemeal while legacy `AsyncPostgreSQLDriver.__init__` still instantiates legacy `SyncPostgreSQLDriver`, breaking Mypy and baseline tests. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch25]` (`[0/33 files documented]` across the full `33`-chapter / `118`-task flow consolidated in `ch30-user-and-workflow-docs`, `ch31-data-and-testing-okf-sync`, and `ch32-ffi-okf-and-review-routing-sync`; Google-style docstrings on `options.py`, `binds.py`, `_sync.py`, and `_async.py` document DSN precedence, lexical rules, and per-command `timeout` boundaries).

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

- **Verified Plan Baseline**: `plan_revision: 6`, `plan_commit: 043d8444`; chapter `state: planned`, `planning_status: complete` (`33` chapters / `118` tasks in full flow, `ch01`–`ch19` / `71` tasks completed).
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending).
- **Downstream Dependents**: Unblocks `ch26-compiler-pipeline-arrow-and-event-hooks` and `ch27-atomic-cutover-and-legacy-removal` ahead of `ch30`–`ch32` documentation/OKF sync (`[0/33 files documented]`) and `ch33-final-review-and-release-gate`.

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native connection constructor kwargs, DSN precedence, lexical bind rules, and permanent driver facades.
[^pg-config-py]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — Existing PostgreSQLConfig fields, from_url, pool_key, pooler modes, and cloud dialer options.
[^pg-core-py]: `src/py/vertebra/db/adapters/postgres/core.py::PostgreSQLCore` — Adapter-owned PostgreSQLCore helpers.
[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Canonical public PostgreSQL driver barrel and static helpers preserved until Chapter 27 activation.
