---
type: Spec
flow_id: driver-fail-loud
parent_flow: driver-fail-loud
chapter_id: ch07-install-hints-and-conformance
title: "Chapter 07: Install Hints, Extras Gate, and Conformance Outcomes"
description: Default MissingDependencyError hints to vertebra-core extras, replace undeclared extras and sentence-valued package names with real pip packages behind an AST extras gate, and make conformance skip a case whose native module is absent, failing it under Docker conformance only for non-pending engines.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch04-redis-valkey-memory-removal
replan_trigger: null
current_task: "driver-fail-loud:ch07:t01"
plan_revision: 1
plan_commit: 0acdf576
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-07T00:00:00Z"
generated:
  by: flow/0.28.0
  at: "2026-11-07T00:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-08T00:00:00Z"
    commit: 0acdf576
sources:
  - id: missing-dep
    resource: src/py/vertebra/_typing.py::MissingDependencyError
    title: Missing dependency error and install hint default
  - id: module-loader
    resource: src/py/vertebra/utils/module_loader.py::ensure_module
    title: Optional module loader and ensure_numpy / ensure_pandas helpers
  - id: litestar-plugin
    resource: src/py/vertebra/extensions/litestar/plugin.py::VertebraPlugin
    title: Litestar plugin raising MissingDependencyError with vertebra[mcp]
  - id: postgres-driver
    resource: src/py/vertebra/db/adapters/postgres/driver.py::AsyncPostgreSQLDriver
    title: PostgreSQL _ensure_pool and _ensure_async_pool dependency guards
  - id: mysql-driver
    resource: src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver
    title: MySQL pymysql and asyncmy dependency guards
  - id: wasm-host
    resource: src/py/vertebra/wasm/__init__.py::WASMPluginHost
    title: WASM plugin host wasmtime dependency guard
  - id: queues-config
    resource: src/py/vertebra/extensions/litestar/queues/config.py::create_sqlspec_queue_config
    title: litestar-queues config helpers with sentence-valued package arguments
  - id: queues-backend
    resource: src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend
    title: litestar-queues SHM backends with sentence-valued package arguments
  - id: saq-plugin
    resource: src/py/vertebra/extensions/litestar/saq/plugin.py::create_saq_plugin
    title: litestar-saq plugin factory with sentence-valued package argument
  - id: mcp-plugin
    resource: src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin
    title: litestar-mcp plugin factory with sentence-valued package argument
  - id: pyproject
    resource: pyproject.toml
    title: Declared project optional-dependencies
  - id: packaging-test
    resource: src/py/tests/unit/build/test_packaging_and_pgo.py
    title: Packaging and extras assertions
  - id: conformance
    resource: src/py/tests/integration/db/test_driver_conformance.py::_resolve_live_config
    title: Conformance live config resolution and pending xfail wrapping
  - id: integration-conftest
    resource: src/py/tests/integration/conftest.py
    title: pytest-databases Docker fixtures for integration tests
tags:
  - packaging
  - install-hints
  - optional-dependencies
  - conformance
  - testing
---

# Chapter 07: Install Hints, Extras Gate, and Conformance Outcomes

<!-- truth: start -->
- `MissingDependencyError(package, *, install_package=None, extra=None)` defaults `install_package` to `vertebra-core[{extra}]` when `extra` is set, else to `package`; `hint` is `Install it with: uv add {install_package}`.[^missing-dep]
- Every string-literal `extra=` passed to `ensure_module` or `MissingDependencyError` under `src/py/vertebra` is a key of `[project.optional-dependencies]`.[^pyproject][^packaging-test]
- Call sites needing a package with no matching extra (`psycopg[binary,pool]`, `pymysql`, `asyncmy`, `wasmtime`, `numpy`, `pandas`) name the pip package and pass no `extra=`.[^postgres-driver][^mysql-driver][^wasm-host][^module-loader]
- `package=` and `install_package=` literals are package names: no whitespace, and any bracketed Vertebra distribution is `vertebra-core`, never `vtb` or `vertebra`.[^queues-config][^queues-backend][^saq-plugin][^mcp-plugin][^litestar-plugin]
- `AsyncPostgreSQLDriver._ensure_async_pool` requires `psycopg_pool` with hint `psycopg[binary,pool]`, matching the sync thread-offload path it guards.[^postgres-driver]
- Conformance resolves the native module before any credential skip; a missing module skips the case in local and `VERTEBRA_CONFORMANCE_DOCKER=1` runs alike when the spec has `pending_flow_id` set, and fails a non-pending case (`bigquery`, `spanner`) under `VERTEBRA_CONFORMANCE_DOCKER=1`.[^conformance]
- Pending engines carry strict `xfail` only when their native module imports, so a missing-module skip or fail is never converted to `xfailed`.[^conformance]
- `src/py/tests/integration/conftest.py` has no `from __future__ import annotations`; `TYPE_CHECKING`-only service types are quoted; the unused `vtb_oracle_driver` and `vtb_mssql_driver` fixtures are gone.[^integration-conftest]
- No engine extras (`postgres`, `mysql`, `oracle`, `mssql`, `sqlite`, `duckdb`, `redis`, `valkey`) are added to `pyproject.toml`; the native driver flows own them.[^pyproject]
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`MissingDependencyError` defaults its install hint to `vtb[<extra>]`, but the distribution is `vertebra-core`, so every hint built from `extra=` alone tells users to install a package that does not exist.[^missing-dep] Seven `extra=` literals name five undeclared extras: `postgres` (`SyncPostgreSQLDriver._ensure_pool`, `AsyncPostgreSQLDriver._ensure_async_pool`) and `mysql` (`SyncMySQLDriver._create_connection`, `AsyncMySQLDriver._ensure_pool`) in the engine drivers, `wasm` in the WASM host, and `numpy` and `pandas` in the module loader.[^pyproject][^postgres-driver][^mysql-driver][^wasm-host][^module-loader] `WASMPluginHost.register_module` and `VertebraPlugin.on_app_init` hard-code `vertebra[wasm]` and `vertebra[mcp]`. Eight call sites pass a sentence such as `"litestar-queues is required for litestar queue integration"` as `package=`, which renders as `Optional dependency 'litestar-queues is required for litestar queue integration' is required but not installed.`[^queues-config][^queues-backend][^saq-plugin][^mcp-plugin] `AsyncPostgreSQLDriver._ensure_async_pool` returns the `asyncpg` module when it imports and otherwise raises for module `psycopg_pool` with package `asyncpg`, while the path it guards runs the sync psycopg pool in a worker thread.[^postgres-driver]

The conformance suite calls `ensure_module` for each spec's native module at the top of `_resolve_live_config` and lets `MissingDependencyError` escape. Non-pending engines (BigQuery, Spanner) report a setup ERROR; pending engines report `xfailed` under their strict `xfail` mark for the wrong reason.[^conformance]

This chapter makes every hint name an installable package, adds a unit gate that fails on any undeclared extra or malformed package literal, and maps a missing native module to an explicit skip, or to an explicit failure in Docker conformance mode for engines that are not pending a native flow.

**Non-goals**: adding engine extras to `pyproject.toml` (owned by each `native-<engine>-driver` flow); changing explicit `install_package="litestar-<x>"` module-level hints that already name real packages; removing redundant function-level `ensure_module` guards in the Litestar integrations; removing `from __future__ import annotations` outside `src/py/tests/integration/conftest.py`; `test_engine_driver_resolution_and_provide_driver` Oracle/MSSQL assertions (owned by ch01 and ch02); memory-branch deletion inside `_ensure_pool` / `_ensure_async_pool` (owned by ch03); OKF and user docs for hints and conformance (owned by [ch09-docs-and-okf](../ch09-docs-and-okf/spec.md)).

### 1.2 Upstream Execution Baseline

- Branch `feat/storage-updates` at `0acdf576`; parent roadmap [spec.md](../../spec.md); catalog [chapters/index.md](../index.md).
- Depends on [ch04-redis-valkey-memory-removal](../ch04-redis-valkey-memory-removal/spec.md), which depends on [ch03-remove-memory-fakes](../ch03-remove-memory-fakes/spec.md): ch03 deletes the `memory://` early returns in `SyncPostgreSQLDriver._ensure_pool` and `AsyncPostgreSQLDriver._ensure_async_pool`, and this chapter edits only the `ensure_module` calls in those methods afterward; ch04 t01 rewrites the cache helpers in `test_driver_conformance.py`, and t04 edits `_resolve_live_config` in the same file afterward.
- Research: [change-surface](../../research/change-surface_20261107.md) sections "Install Hints and Extras" and "Conformance".

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Error type | `src/py/vertebra/_typing.py::MissingDependencyError`[^missing-dep] | In-process; `ImportError` subclass re-exported from `vertebra.typing`; `install_package`, `hint`, `package` attributes |
| Loader | `src/py/vertebra/utils/module_loader.py::ensure_module`, `ensure_numpy`, `ensure_pandas`[^module-loader] | In-process; `ensure_module` forwards `package`, `extra`, `install_package` to the error |
| Litestar plugin | `src/py/vertebra/extensions/litestar/plugin.py::VertebraPlugin.on_app_init`[^litestar-plugin] | Raises with `install_package="vertebra[mcp]"` when `litestar_mcp` lacks `LitestarMCP` / `LitestarA2A` |
| PostgreSQL | `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver._ensure_pool`, `AsyncPostgreSQLDriver._ensure_async_pool`[^postgres-driver] | External boundary (`psycopg_pool`); async guard return value is discarded by `AsyncPostgreSQLDriver._run_async_query`, which then calls `self._sync._run_sync_query` in a thread |
| MySQL | `src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver._create_connection`, `AsyncMySQLDriver._ensure_pool`[^mysql-driver] | External boundary (`pymysql`, `asyncmy`) |
| WASM | `src/py/vertebra/wasm/__init__.py::WASMPluginHost.register_module`[^wasm-host] | External boundary (`wasmtime`, resolved at import through `import_optional`) |
| Litestar integrations | `queues/config.py::create_sqlspec_queue_config`, `create_shm_queue_config`[^queues-config]; `queues/backend.py::SHMQueueBackend.__init__`, `SHMExecutionBackend.__init__`[^queues-backend]; `saq/plugin.py::create_saq_plugin`[^saq-plugin]; `mcp/plugin.py::create_mcp_plugin`[^mcp-plugin] | Function-level `ensure_module` guards with sentence-valued `package=` |
| Extras declaration | `pyproject.toml` `[project.optional-dependencies]`[^pyproject] | Declared keys: `sqlspec`, `alchemy`, `arrow`, `pydantic`, `attrs`, `polars`, `litestar`, `starlette`, `fastapi`, `flask`, `django`, `queues`, `security`, `autowire`, `email`, `mcp`, `vite`, `asyncapi`, `htmx`, `saq`, `embeddings`, `bigquery`, `bundler`, `spanner`, `storage`, `rloop`, `all` |
| Packaging gate | `src/py/tests/unit/build/test_packaging_and_pgo.py`[^packaging-test] | Unit test seam reading `pyproject.toml` with `tomllib` |
| Conformance | `src/py/tests/integration/db/test_driver_conformance.py::EngineConformanceSpec`, `ENGINE_SPECS`, `_build_live_engine_params`, `_resolve_live_config`, `sync_sql_driver`, `async_sql_driver`[^conformance] | Test seam; fixtures parametrized by `_build_live_engine_params()` call `_resolve_live_config` first |
| Integration fixtures | `src/py/tests/integration/conftest.py`[^integration-conftest] | pytest-databases service fixtures annotated with `TYPE_CHECKING`-only types; `vtb_oracle_driver` and `vtb_mssql_driver` have no consumers under `src/py/tests` |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Database drivers and dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md)
- [Polyglot test matrix](../../../../knowledge/testing/polyglot-test-matrix.md)
- [ADR-004 optional driver wheels](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md)
- [Project workflow](../../../../knowledge/workflow.md)
- [python-framework skill](../../../../../skills/python-framework/SKILL.md)

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R10 (`t01`, `t02`, `t03`)**: `MissingDependencyError` defaults its hint to `vertebra-core[<extra>]`; every `extra=` literal in `src/py/vertebra` is declared in `[project.optional-dependencies]`; sentence-valued `package=` arguments become package names.
- **R11 (`t03`)**: A unit test fails when `src/py/vertebra` references an undeclared extra, a `package=` or `install_package=` literal containing whitespace, or a bracketed Vertebra distribution other than `vertebra-core`.
- **R12 (`t04`)**: Conformance resolves the native module before credential skips; a case whose native module is missing skips when its spec has `pending_flow_id` set (local and `VERTEBRA_CONFORMANCE_DOCKER=1` alike), skips a non-pending spec locally, and fails a non-pending spec when `VERTEBRA_CONFORMANCE_DOCKER=1`; strict `xfail` applies only when a pending engine's module imports.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
class MissingDependencyError(VertebraError, ImportError):
    """Raised when an optional dependency is required but unavailable."""

    def __init__(
        self,
        package: str,
        *,
        install_package: str | None = None,
        extra: str | None = None,
    ) -> None:
        self.package = package
        self.install_package = install_package or (f"vertebra-core[{extra}]" if extra else package)
        self.message = f"Optional dependency '{package}' is required but not installed."
        self.hint = f"Install it with: uv add {self.install_package}"
        super().__init__(self.message)
```

Call-site literals after this chapter:

| Symbol | Call |
| --- | --- |
| `module_loader.py::ensure_numpy` | `ensure_module("numpy")` |
| `module_loader.py::ensure_pandas` | `ensure_module("pandas")` |
| `litestar/plugin.py::VertebraPlugin.on_app_init` (both branches) | `MissingDependencyError("litestar-mcp", extra="mcp")` |
| `postgres/driver.py::SyncPostgreSQLDriver._ensure_pool` | `ensure_module("psycopg_pool", install_package="psycopg[binary,pool]")` |
| `postgres/driver.py::AsyncPostgreSQLDriver._ensure_async_pool` | `return ensure_module("psycopg_pool", install_package="psycopg[binary,pool]")`; the `import_optional("asyncpg")` probe is deleted |
| `mysql/driver.py::SyncMySQLDriver._create_connection` | `ensure_module("pymysql")` |
| `mysql/driver.py::AsyncMySQLDriver._ensure_pool` | `ensure_module("asyncmy")` |
| `wasm/__init__.py::WASMPluginHost.register_module` | `raise MissingDependencyError("wasmtime")` |
| `queues/config.py`, `queues/backend.py` (six calls) | `ensure_module(<module>, package="litestar-queues", extra="queues")` |
| `saq/plugin.py::create_saq_plugin` | `ensure_module("litestar_saq", package="litestar-saq", extra="saq")` |
| `mcp/plugin.py::create_mcp_plugin` | `ensure_module("litestar_mcp", package="litestar-mcp", extra="mcp")` |

Packaging gate helper in `src/py/tests/unit/build/test_packaging_and_pgo.py`:

```python
_DEPENDENCY_CALLEES: frozenset[str] = frozenset({"ensure_module", "MissingDependencyError"})
_BRACKETED_DISTRIBUTION = re.compile(r"^(?P<dist>[A-Za-z0-9_.-]+)\[(?P<extra>[^\]]+)\]$")


def _optional_dependency_violations(
    source_root: Path,
    declared: frozenset[str],
    *,
    relative_to: Path,
) -> list[str]:
    """Return malformed optional-dependency literals under ``source_root``.

    Args:
        source_root: Directory scanned recursively for ``*.py`` files.
        declared: Keys of ``[project.optional-dependencies]``.
        relative_to: Base directory for the reported paths.

    Returns:
        Sorted ``"<path>:<line>::<callee> <keyword>=<repr(value)>"`` entries, empty when every literal is valid.
    """
```

Conformance helpers in `src/py/tests/integration/db/test_driver_conformance.py`:

```python
_CONFORMANCE_DOCKER_ENV = "VERTEBRA_CONFORMANCE_DOCKER"


def _native_module_missing(spec: EngineConformanceSpec) -> bool:
    """Return True when ``spec`` names a native module that does not import."""


def _require_native_module(spec: EngineConformanceSpec) -> None:
    """Skip the case, or fail a non-pending case under Docker conformance, when the native module is missing.

    Raises:
        pytest.skip.Exception: When the module is missing and either the spec is pending or
            ``VERTEBRA_CONFORMANCE_DOCKER`` is not ``"1"``.
        pytest.fail.Exception: When the module is missing, the spec is not pending, and
            ``VERTEBRA_CONFORMANCE_DOCKER`` is ``"1"``.
    """


def _build_live_engine_params(specs: Sequence[EngineConformanceSpec] = ENGINE_SPECS) -> list[Any]:
    """Build fixture parameters, marking pending engines strict ``xfail`` only when their native module imports."""
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `ensure_module(name, package=..., extra=..., install_package=...)` → `import_optional(name)` returns `None` → `MissingDependencyError(package or name, install_package=..., extra=...)` → `install_package` / `hint`.[^module-loader][^missing-dep]
- `AsyncPostgreSQLDriver._run_async_query` (no executor, no transaction connection, no pool) → `_ensure_async_pool()` → `ensure_module("psycopg_pool", install_package="psycopg[binary,pool]")` → `asyncio.to_thread(self._sync._run_sync_query, ...)` → `SyncPostgreSQLDriver._ensure_pool()`.[^postgres-driver]
- `test_optional_dependency_literals_name_declared_extras` → `tomllib.loads(pyproject.toml)["project"]["optional-dependencies"]` → `_optional_dependency_violations(repo_root / "src/py/vertebra", declared, relative_to=repo_root)` → `ast.parse` per file → `ast.Call` nodes whose callee is `ensure_module` / `MissingDependencyError`.[^packaging-test]
- pytest collection → `_build_live_engine_params()` → `_native_module_missing(spec)` → `import_optional(spec.native_extension_module)` → `pytest.param(spec, marks=...)`.[^conformance]
- `sync_sql_driver` / `async_sql_driver` → `_resolve_live_config(spec, tmp_path, request)` → `_require_native_module(spec)` → `pytest.skip(reason)`, or `pytest.fail(reason, pytrace=False)` when `spec.pending_flow_id is None` and `VERTEBRA_CONFORMANCE_DOCKER == "1"` → BigQuery / Spanner credential checks → env DSN → Docker fixture → `sample_url`.[^conformance]

### 2.4 Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Extra only | `MissingDependencyError("x", extra="sqlspec")` | `install_package == "vertebra-core[sqlspec]"`; `hint == "Install it with: uv add vertebra-core[sqlspec]"` | No `vtb[` hint anywhere |
| Explicit package wins | `MissingDependencyError("x", install_package="pkg", extra="sqlspec")` | `install_package == "pkg"` | Explicit override unchanged |
| Neither | `MissingDependencyError("x")` | `install_package == "x"` | Package fallback unchanged |
| `numpy` absent | `sys.modules["numpy"] = None`, `ensure_numpy()` | `MissingDependencyError`, `install_package == "numpy"` | No undeclared extra |
| Async PostgreSQL without `asyncpg` or `psycopg_pool` | `sys.modules["asyncpg"] = None`, `sys.modules["psycopg_pool"] = None`; `AsyncPostgreSQLDriver(cfg).execute_tuples("SELECT 1")` | `MissingDependencyError`, `package == "psycopg_pool"`, `install_package == "psycopg[binary,pool]"` (today `package == "asyncpg"`) | Guard names the module the thread-offload path imports |
| Async PostgreSQL with `asyncpg` but no `psycopg_pool` | `AsyncPostgreSQLDriver(cfg).execute_tuples("SELECT 1")` with `SyncPostgreSQLDriver._run_sync_query` patched to raise `AssertionError` | `MissingDependencyError`, `package == "psycopg_pool"`, `install_package == "psycopg[binary,pool]"`, raised by `_ensure_async_pool` before thread offload | Guard checks the module before the offload, not inside the thread |
| `wasmtime` absent | `wasm_mod._wasmtime = None`, `register_module(cfg)` without exports | `install_package == "wasmtime"` | No `vertebra[` hint |
| Sentence package | `ensure_module("litestar_queues", package="litestar-queues is required ...")` in source | Gate reports `<path>:<line>::ensure_module package='litestar-queues is required ...'` | Gate catches whitespace in package literals |
| Sentence install package | `MissingDependencyError("redis", install_package="redis is required")` in source | Gate reports `<path>:<line>::MissingDependencyError install_package='redis is required'` | Gate catches whitespace in `install_package` literals |
| Non-literal extra | `ensure_module(name, extra=extra)` inside `ensure_module` forwarding | Ignored by gate | Only literals are checked |
| Unrelated `extra=` | `logger.info("x", extra={...})` | Ignored: callee not in `_DEPENDENCY_CALLEES` | No false positives |
| Pending native extra hint | `install_package="vertebra-core[oracle]"` (ch01 resolvers) | Accepted: gate checks the bracketed distribution name, not the bracketed extra | ch01 resolvers stay green until native flows declare `oracle` / `mssql` |
| Pending module missing, local | `vertebra._postgres` absent, env unset | Case reports SKIPPED with `postgres conformance requires the vertebra._postgres native module` | No setup ERROR, no `xfailed` |
| Pending module missing, Docker | same, `VERTEBRA_CONFORMANCE_DOCKER=1` | Case reports SKIPPED with the same reason | Docker runs stay green until each native flow ships and clears `pending_flow_id` |
| DuckDB case | `duckdb` package installed, `vertebra._duckdb` absent | SKIPPED with `duckdb conformance requires the vertebra._duckdb native module` in both modes | DuckDB conformance stays pending on the native module |
| Non-pending module missing, local | `vertebra._bigquery` absent, env unset | SKIPPED with `bigquery conformance requires the vertebra._bigquery native module` | No setup ERROR |
| Non-pending module missing, Docker | same, `VERTEBRA_CONFORMANCE_DOCKER=1` | Case reports ERROR at setup with the same reason; exit code non-zero | No silent pass for shipped native modules |
| Native module present, pending | `vertebra._postgres` imports, `pending_flow_id="native-postgres-driver"` | Strict `xfail` with `Owned by native-postgres-driver` | XPASS still fails |
| Cloud engine without credentials | `vertebra._bigquery` absent, no DSN | Native-module skip, raised before the `VERTEBRA_CONFORMANCE_BIGQUERY_DSN` skip | Module resolution first |
| Docker env other than `"1"` | `VERTEBRA_CONFORMANCE_DOCKER=0` or `true` | Skip | Exact-match `"1"` like the Docker fixture branch |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given `MissingDependencyError("x", extra="sqlspec")`, when `hint` is read, then it equals `Install it with: uv add vertebra-core[sqlspec]`.
- Given a source file containing `ensure_module("wasmtime", extra="wasm")` and `wasm` absent from the declared extras, when `_optional_dependency_violations` scans it, then it returns `["pkg/mod.py:1::ensure_module extra='wasm'"]`.
- Given the repository after this chapter, when `test_optional_dependency_literals_name_declared_extras` runs, then the violation list is `[]`.
- Given `asyncpg` and `psycopg_pool` absent, when `AsyncPostgreSQLDriver(PostgreSQLConfig(dsn="postgresql://unreachable-host:5432/db")).execute_tuples("SELECT 1")` runs, then `MissingDependencyError.package == "psycopg_pool"` and `install_package == "psycopg[binary,pool]"`.
- Given `asyncpg` importable, `psycopg_pool` absent, and `SyncPostgreSQLDriver._run_sync_query` patched to raise `AssertionError`, when the same call runs, then `MissingDependencyError` with `install_package == "psycopg[binary,pool]"` is raised and the patched method is never called.
- Given `vertebra._bigquery` absent and no BigQuery DSN, when `_resolve_live_config` runs for the BigQuery spec, then `pytest.skip` fires with `bigquery conformance requires the vertebra._bigquery native module`.
- Given the same with `VERTEBRA_CONFORMANCE_DOCKER=1`, then `pytest.fail` fires with the same reason.
- Given `vertebra._postgres` absent and `VERTEBRA_CONFORMANCE_DOCKER=1`, when `_resolve_live_config` runs for the pending PostgreSQL spec, then `pytest.skip` fires with `postgres conformance requires the vertebra._postgres native module`.

### 2.6 Alternatives Considered

| Alternative | Depth / Locality / Seam | Outcome |
| --- | --- | --- |
| Declare interim `postgres`, `mysql`, `wasm`, `numpy`, `pandas` extras | Keeps call sites unchanged; extras collide with native wheel extras owned by the native driver flows | Rejected: root non-goal; hints name pip packages directly |
| Regex grep gate over source text | Shallow; misses multi-line calls and attribute callees such as `module_loader.ensure_module(...)`, false-positives on logging `extra=` | Rejected: AST walk on `ast.Call` with callee filter and literal check |
| Gate also requires bracketed extras in `install_package` to be declared | Strict truthfulness; flags ch01's `vertebra-core[oracle]` / `vertebra-core[mssql]` until native flows ship | Rejected for this chapter: gate checks the bracketed distribution name; the extra-declared rule applies to `extra=` per R11 |
| Keep `asyncpg` probe in `_ensure_async_pool` and only fix the package name | Guard passes when `asyncpg` is present, then the thread-offload path raises for `psycopg_pool` | Rejected: the guard must check the module the guarded path imports |
| Fail every engine, pending or not, under `VERTEBRA_CONFORMANCE_DOCKER=1` | Docker conformance fails on every host until all five native flows ship | Rejected: pending engines skip; the failure applies once a native flow clears `pending_flow_id` |
| Catch `MissingDependencyError` and call `pytest.fail` while keeping strict `xfail` on pending specs | pytest reports a setup failure under an `xfail` mark as `xfailed`, so Docker mode would silently pass | Rejected: apply strict `xfail` only when the native module imports |
| Collection-time `pytest.mark.skip` for missing modules | Skip is decided at collection; Docker mode cannot turn it into a failure | Rejected: runtime `_require_native_module` decides skip or fail |
| Helper unit tests in a new `src/py/tests/unit/` module | Must import private helpers from an integration test module | Rejected: tests live beside the private helpers in `test_driver_conformance.py`, which `make py-test` already collects |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch07]`
- Install-hint and conformance-outcome prose in `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` is owned by [ch09-docs-and-okf](../ch09-docs-and-okf/spec.md); this chapter changes no user docs. Python docstrings on touched helpers follow Google style.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R10 (hint default, loader, Litestar plugin) | [t01](tasks/t01-missing-dependency-hint-default.md) | `behavior_tdd` | `src/py/tests/unit/framework/test_typing.py`, `src/py/tests/unit/framework/test_framework_subdomains.py` |
| R10 (engine and WASM hints) | [t02](tasks/t02-engine-and-wasm-install-hints.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/tests/unit/db/test_mysql_driver.py`, `src/py/tests/unit/extensions/test_grpc_service.py` |
| R10 (sentence packages), R11 | [t03](tasks/t03-package-names-and-extras-gate.md) | `behavior_tdd` | `src/py/tests/unit/build/test_packaging_and_pgo.py`, `src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py` |
| R12 | [t04](tasks/t04-conformance-outcomes.md) | `behavior_tdd` | `src/py/tests/integration/db/test_driver_conformance.py` |

## 5. Implementation Plan

- [ ] driver-fail-loud:ch07:t01 — `MissingDependencyError` defaults to `vertebra-core[<extra>]`; loader and Litestar plugin hints name real packages (tasks/t01-missing-dependency-hint-default.md)
- [ ] driver-fail-loud:ch07:t02 — PostgreSQL, MySQL, and WASM guards name pip packages without undeclared extras; async PostgreSQL guard checks `psycopg_pool` (tasks/t02-engine-and-wasm-install-hints.md)
- [ ] driver-fail-loud:ch07:t03 — Litestar integrations pass package names; AST gate rejects undeclared extras and malformed package literals (tasks/t03-package-names-and-extras-gate.md)
- [ ] driver-fail-loud:ch07:t04 — Conformance skips or fails explicitly on a missing native module; integration conftest drops `from __future__` and the unused Oracle / MSSQL driver fixtures (tasks/t04-conformance-outcomes.md)
- **Chapter Recovery Checkpoint**: `make lint && uv run --no-sync pytest src/py/tests/unit/framework/test_typing.py src/py/tests/unit/framework/test_framework_subdomains.py src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_mysql_driver.py src/py/tests/unit/extensions/test_grpc_service.py src/py/tests/unit/build/test_packaging_and_pgo.py src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py src/py/tests/integration/db/test_driver_conformance.py -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `0acdf576` — chapter planned; `0/4` tasks completed; waits on `ch03-remove-memory-fakes`.
- **Remaining Tasks**: `t01`, `t02`, `t03` (depends on `t01`, `t02`), `t04`.
- **Documentation & OKF Sync**: none owned; `ch09-docs-and-okf` documents hints and conformance outcomes.
- **Active Invariants**: no engine extras in `pyproject.toml`; `install_package` overrides still win over `extra`; strict `xfail` remains for pending engines whose native module imports; pending engines skip a missing module in Docker mode; `test_engine_driver_resolution_and_provide_driver` stays owned by ch01 / ch02.

[^missing-dep]: `src/py/vertebra/_typing.py::MissingDependencyError` — Missing dependency error and install hint default.
[^module-loader]: `src/py/vertebra/utils/module_loader.py::ensure_module` — Optional module loader and ensure_numpy / ensure_pandas helpers.
[^litestar-plugin]: `src/py/vertebra/extensions/litestar/plugin.py::VertebraPlugin` — Litestar plugin raising MissingDependencyError with vertebra[mcp].
[^postgres-driver]: `src/py/vertebra/db/adapters/postgres/driver.py::AsyncPostgreSQLDriver` — PostgreSQL _ensure_pool and _ensure_async_pool dependency guards.
[^mysql-driver]: `src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver` — MySQL pymysql and asyncmy dependency guards.
[^wasm-host]: `src/py/vertebra/wasm/__init__.py::WASMPluginHost` — WASM plugin host wasmtime dependency guard.
[^queues-config]: `src/py/vertebra/extensions/litestar/queues/config.py::create_sqlspec_queue_config` — litestar-queues config helpers with sentence-valued package arguments.
[^queues-backend]: `src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend` — litestar-queues SHM backends with sentence-valued package arguments.
[^saq-plugin]: `src/py/vertebra/extensions/litestar/saq/plugin.py::create_saq_plugin` — litestar-saq plugin factory with sentence-valued package argument.
[^mcp-plugin]: `src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin` — litestar-mcp plugin factory with sentence-valued package argument.
[^pyproject]: `pyproject.toml` — Declared project optional-dependencies.
[^packaging-test]: `src/py/tests/unit/build/test_packaging_and_pgo.py` — Packaging and extras assertions.
[^conformance]: `src/py/tests/integration/db/test_driver_conformance.py::_resolve_live_config` — Conformance live config resolution and pending xfail wrapping.
[^integration-conftest]: `src/py/tests/integration/conftest.py` — pytest-databases Docker fixtures for integration tests.
