---
type: Spec
flow_id: driver-fail-loud
parent_flow: driver-fail-loud
chapter_id: ch08-install-hints-and-extras-gate
title: "Chapter 08: Install Hints and Extras Gate"
description: Default MissingDependencyError hints to vertebra-core extras and replace undeclared extras and sentence-valued package names with real pip packages behind an AST extras gate.
status: stable
state: completed
planning_status: complete
depends_on_chapters:
  - ch04-redis-valkey-memory-removal
replan_trigger: null
current_task: null
plan_revision: 2
plan_commit: c2b09b9b
state_revision: 1
last_verified_checkpoint: 9a60db2c
last_reviewed_commit: null
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-09T00:00:00Z"
generated:
  by: flow/0.28.0
  at: "2026-11-07T00:00:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-09T14:45:00Z"
    commit: 9a60db2c
sources:
  - id: missing-dep
    resource: src/py/vertebra/_typing.py::MissingDependencyError
    title: Missing dependency error and vertebra-core[extra] install hint default
  - id: module-loader
    resource: src/py/vertebra/utils/module_loader.py::ensure_module
    title: Optional module loader and ensure_numpy / ensure_pandas helpers
  - id: litestar-plugin
    resource: src/py/vertebra/extensions/litestar/plugin.py::VertebraPlugin
    title: Litestar plugin raising MissingDependencyError with vertebra-core[mcp]
  - id: postgres-driver
    resource: src/py/vertebra/db/adapters/postgres/driver.py::AsyncPostgreSQLDriver
    title: PostgreSQL _ensure_pool and _ensure_async_pool psycopg[binary,pool] guards
  - id: mysql-driver
    resource: src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver
    title: MySQL pymysql and asyncmy dependency guards
  - id: wasm-host
    resource: src/py/vertebra/wasm/__init__.py::WASMPluginHost
    title: WASM plugin host wasmtime dependency guard
  - id: queues-config
    resource: src/py/vertebra/extensions/litestar/queues/config.py::create_sqlspec_queue_config
    title: litestar-queues config helpers naming litestar-queues as package
  - id: queues-backend
    resource: src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend
    title: litestar-queues SHM backends naming litestar-queues as package
  - id: saq-plugin
    resource: src/py/vertebra/extensions/litestar/saq/plugin.py::create_saq_plugin
    title: litestar-saq plugin factory naming litestar-saq as package
  - id: mcp-plugin
    resource: src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin
    title: litestar-mcp plugin factory naming litestar-mcp as package
  - id: pyproject
    resource: pyproject.toml
    title: Declared project optional-dependencies
  - id: packaging-test
    resource: src/py/tests/unit/build/test_packaging_and_pgo.py
    title: Packaging and extras assertions
  - id: saq-tests
    resource: src/py/tests/unit/extensions/test_litestar_ecosystem_frontend_and_saq.py::test_missing_dependency_errors
    title: SAQ missing-dependency test extended with a create_saq_plugin assertion
  - id: mcp-tests
    resource: src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py::test_missing_dependency_error_on_missing_mcp
    title: MCP missing-dependency test extended with a create_mcp_plugin assertion
tags:
  - packaging
  - install-hints
  - optional-dependencies
  - gate
---

# Chapter 08: Install Hints and Extras Gate

<!-- truth: start -->
- `MissingDependencyError(package, *, install_package=None, extra=None)` defaults `install_package` to `vertebra-core[{extra}]` when `extra` is set, else to `package`; `hint` is `Install it with: uv add {install_package}`.[^missing-dep]
- Every string-literal `extra=` passed to `ensure_module` or `MissingDependencyError` under `src/py/vertebra` is a key of `[project.optional-dependencies]`.[^pyproject][^packaging-test]
- Call sites needing a package with no matching extra (`psycopg[binary,pool]`, `pymysql`, `asyncmy`, `wasmtime`, `numpy`, `pandas`) name the pip package and pass no `extra=`.[^postgres-driver][^mysql-driver][^wasm-host][^module-loader]
- `package=` and `install_package=` literals are package names: no whitespace, and any bracketed Vertebra distribution is `vertebra-core`, never `vtb` or `vertebra`.[^queues-config][^queues-backend][^saq-plugin][^mcp-plugin][^litestar-plugin]
- `AsyncPostgreSQLDriver._ensure_async_pool` requires `psycopg_pool` with hint `psycopg[binary,pool]`, matching the sync thread-offload path it guards.[^postgres-driver]
- `create_saq_plugin` and `create_mcp_plugin` raise `MissingDependencyError` with `package == "litestar-saq"` / `"litestar-mcp"` when their module is absent.[^saq-tests][^mcp-tests]
- `test_optional_dependency_literals_name_declared_extras` walks every `src/py/vertebra/**/*.py` AST and reports `[]`; its self-test pins expected violations including undeclared extras, malformed package literals, and unhinted `vertebra._*` companion extension imports.[^packaging-test]
- No engine extras (`postgres`, `mysql`, `oracle`, `mssql`, `sqlite`, `duckdb`, `redis`, `valkey`) are added to `pyproject.toml`; the native driver flows own them.[^pyproject]
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`MissingDependencyError` defaults `install_package` to `vertebra-core[<extra>]` when `extra=` is supplied, and every `ensure_module` and `MissingDependencyError` call site under `src/py/vertebra` names either a declared `[project.optional-dependencies]` extra or an installable pip package (`psycopg[binary,pool]`, `pymysql`, `asyncmy`, `wasmtime`, `numpy`, `pandas`, `litestar-queues`, `litestar-saq`, `litestar-mcp`, `vertebra-core[mcp]`, `vertebra-core[bundler]`) without sentence-valued `package=` strings or `vtb[...]` / `vertebra[...]` distribution names.[^missing-dep][^pyproject][^postgres-driver][^mysql-driver][^wasm-host][^module-loader][^queues-config][^queues-backend][^saq-plugin][^mcp-plugin][^litestar-plugin] `AsyncPostgreSQLDriver._ensure_async_pool` resolves `psycopg_pool` with `install_package="psycopg[binary,pool]"`, matching the sync thread-offload pool path it guards, and `test_optional_dependency_literals_name_declared_extras` enforces these constraints across every `src/py/vertebra/**/*.py` AST.[^postgres-driver][^packaging-test]

**Non-goals**: adding engine extras to `pyproject.toml` (owned by each `native-<engine>-driver` flow); changing explicit `install_package="litestar-<x>"` module-level hints that already name real packages; removing redundant function-level `ensure_module` guards in the Litestar integrations; removing `from __future__ import annotations` outside `src/py/tests/integration/conftest.py`; `test_engine_driver_resolution_and_provide_driver` Oracle/MSSQL assertions (owned by ch01 and ch02); memory-branch deletion inside `_ensure_pool` / `_ensure_async_pool` (owned by ch03); conformance outcomes for missing native modules and the integration conftest cleanup (owned by [ch09-conformance-outcomes](../ch09-conformance-outcomes/spec.md)); OKF and user docs for hints and conformance (owned by [ch11-docs-and-okf](../ch11-docs-and-okf/spec.md)).

### 1.2 Upstream Execution Baseline

- Branch `feat/driver-fail-loud` stacked on `feat/storage-updates` (`d2b168dc`); parent roadmap [spec.md](../../spec.md); catalog [chapters/index.md](../index.md).
- Depends on [ch04-redis-valkey-memory-removal](../ch04-redis-valkey-memory-removal/spec.md), which depends on [ch03-remove-memory-fakes](../ch03-remove-memory-fakes/spec.md). The file overlap is with ch03: ch03 `t01` deletes the `memory://` early returns in `SyncPostgreSQLDriver._ensure_pool`, `AsyncPostgreSQLDriver._ensure_async_pool`, and the MySQL drivers, which `t02` edits afterward; ch03 `t03` retargets `test_framework_subdomains.py`, which `t01` extends afterward. Declaring ch04 keeps the whole memory-removal sequence ahead of this chapter.
- No file overlap with ch05, ch06, ch07, or [ch09-conformance-outcomes](../ch09-conformance-outcomes/spec.md); those chapters do not wait on this one. [ch10-queue-backend-fail-loud](../ch10-queue-backend-fail-loud/spec.md) depends on this chapter for the hint default and the gate.
- Research: [change-surface](../../research/change-surface_20261107.md) sections "Install Hints and Extras" and "Conformance".

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Error type | `src/py/vertebra/_typing.py::MissingDependencyError`[^missing-dep] | In-process; `ImportError` subclass re-exported from `vertebra.typing`; `install_package`, `hint`, `package` attributes |
| Loader | `src/py/vertebra/utils/module_loader.py::ensure_module`, `ensure_numpy`, `ensure_pandas`[^module-loader] | In-process; `ensure_module` forwards `package`, `extra`, `install_package` to the error |
| Litestar plugin | `src/py/vertebra/extensions/litestar/plugin.py::VertebraPlugin.on_app_init`[^litestar-plugin] | Raises with `install_package="vertebra-core[mcp]"` when `litestar_mcp` lacks `LitestarMCP` / `LitestarA2A` |
| PostgreSQL | `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver._ensure_pool`, `AsyncPostgreSQLDriver._ensure_async_pool`[^postgres-driver] | External boundary (`psycopg_pool`); async guard return value is discarded by `AsyncPostgreSQLDriver._run_async_query`, which then calls `self._sync._run_sync_query` in a thread |
| MySQL | `src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver._create_connection`, `AsyncMySQLDriver._ensure_pool`[^mysql-driver] | External boundary (`pymysql`, `asyncmy`) |
| WASM | `src/py/vertebra/wasm/__init__.py::WASMPluginHost.register_module`[^wasm-host] | External boundary (`wasmtime`, resolved at import through `import_optional`) |
| Litestar integrations | `queues/config.py::create_sqlspec_queue_config`, `create_shm_queue_config`[^queues-config]; `queues/backend.py::SHMQueueBackend.__init__`, `SHMExecutionBackend.__init__`[^queues-backend]; `saq/plugin.py::create_saq_plugin`[^saq-plugin]; `mcp/plugin.py::create_mcp_plugin`[^mcp-plugin] | Function-level `ensure_module` guards with sentence-valued `package=` (six in queues, one each in SAQ and MCP) |
| Litestar ecosystem tests | `test_litestar_ecosystem_frontend_and_saq.py::test_missing_dependency_errors`[^saq-tests]; `test_litestar_ecosystem_mcp.py::test_missing_dependency_error_on_missing_mcp`[^mcp-tests]; `test_litestar_ecosystem_queues.py::test_missing_dependency_error_on_missing_queues` | Patch `vertebra.utils.module_loader.import_optional` to `lambda name: None`; today they call `ensure_module` directly rather than the factories |
| Extras declaration | `pyproject.toml` `[project.optional-dependencies]`[^pyproject] | Declared keys: `sqlspec`, `alchemy`, `arrow`, `pydantic`, `attrs`, `polars`, `litestar`, `starlette`, `fastapi`, `flask`, `django`, `queues`, `security`, `autowire`, `email`, `mcp`, `vite`, `asyncapi`, `htmx`, `saq`, `embeddings`, `bigquery`, `bundler`, `spanner`, `storage`, `rloop`, `all` |
| Packaging gate | `src/py/tests/unit/build/test_packaging_and_pgo.py`[^packaging-test] | Unit test seam reading `pyproject.toml` with `tomllib` |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Database drivers and dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md)
- [ADR-004 optional driver wheels](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md)
- [Project workflow](../../../../knowledge/workflow.md)
- [python-framework skill](../../../../../skills/python-framework/SKILL.md)

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R10 (`t01`, `t02`, `t03`, `t04`)**: `MissingDependencyError` defaults its hint to `vertebra-core[<extra>]`; every `extra=` literal in `src/py/vertebra` is declared in `[project.optional-dependencies]`; sentence-valued `package=` arguments become package names.
- **R11 (`t04`)**: A unit test fails when `src/py/vertebra` references an undeclared extra, a `package=` or `install_package=` literal containing whitespace, or a bracketed Vertebra distribution other than `vertebra-core`.
- R12 (conformance outcomes) is owned by [ch09-conformance-outcomes](../ch09-conformance-outcomes/spec.md).

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

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `ensure_module(name, package=..., extra=..., install_package=...)` → `import_optional(name)` returns `None` → `MissingDependencyError(package or name, install_package=..., extra=...)` → `install_package` / `hint`.[^module-loader][^missing-dep]
- `AsyncPostgreSQLDriver._run_async_query` (no executor, no transaction connection, no pool) → `_ensure_async_pool()` → `ensure_module("psycopg_pool", install_package="psycopg[binary,pool]")` → `asyncio.to_thread(self._sync._run_sync_query, ...)` → `SyncPostgreSQLDriver._ensure_pool()`.[^postgres-driver]
- `test_optional_dependency_literals_name_declared_extras` → `tomllib.loads(pyproject.toml)["project"]["optional-dependencies"]` → `_optional_dependency_violations(repo_root / "src/py/vertebra", declared, relative_to=repo_root)` → `ast.parse` per file → `ast.Call` nodes whose callee is `ensure_module` / `MissingDependencyError`.[^packaging-test]
- `test_missing_dependency_errors` / `test_missing_dependency_error_on_missing_mcp` → `create_saq_plugin(...)` / `create_mcp_plugin(...)` → `ensure_module("litestar_<x>", package="litestar-<x>", extra="<x>")` → patched `import_optional` returns `None` → `MissingDependencyError`.[^saq-tests][^mcp-tests][^saq-plugin][^mcp-plugin]

### 2.4 Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Extra only | `MissingDependencyError("x", extra="sqlspec")` | `install_package == "vertebra-core[sqlspec]"`; `hint == "Install it with: uv add vertebra-core[sqlspec]"` | No `vtb[` hint anywhere |
| Explicit package wins | `MissingDependencyError("x", install_package="pkg", extra="sqlspec")` | `install_package == "pkg"` | Explicit override unchanged |
| Neither | `MissingDependencyError("x")` | `install_package == "x"` | Package fallback unchanged |
| `numpy` absent | `sys.modules["numpy"] = None`, `ensure_numpy()` | `MissingDependencyError`, `install_package == "numpy"` | No undeclared extra |
| Async PostgreSQL without `asyncpg` or `psycopg_pool` | `sys.modules["asyncpg"] = None`, `sys.modules["psycopg_pool"] = None`; `AsyncPostgreSQLDriver(cfg).execute_tuples("SELECT 1")` | `MissingDependencyError`, `package == "psycopg_pool"`, `install_package == "psycopg[binary,pool]"` | Guard names the module the thread-offload path imports |
| Async PostgreSQL with `asyncpg` but no `psycopg_pool` | `AsyncPostgreSQLDriver(cfg).execute_tuples("SELECT 1")` with `SyncPostgreSQLDriver._run_sync_query` patched to raise `AssertionError` | `MissingDependencyError`, `package == "psycopg_pool"`, `install_package == "psycopg[binary,pool]"`, raised by `_ensure_async_pool` before thread offload | Guard checks the module before the offload, not inside the thread |
| `wasmtime` absent | `wasm_mod._wasmtime = None`, `register_module(cfg)` without exports | `install_package == "wasmtime"` | No `vertebra[` hint |
| Sentence package | `ensure_module("litestar_queues", package="litestar-queues is required ...")` in source | Gate reports `<path>:<line>::ensure_module package='litestar-queues is required ...'` | Gate catches whitespace in package literals |
| Sentence install package | `MissingDependencyError("redis", install_package="redis is required")` in source | Gate reports `<path>:<line>::MissingDependencyError install_package='redis is required'` | Gate catches whitespace in `install_package` literals |
| Non-literal extra | `ensure_module(name, extra=extra)` inside `ensure_module` forwarding | Ignored by gate | Only literals are checked |
| Unrelated `extra=` | `logger.info("x", extra={...})` | Ignored: callee not in `_DEPENDENCY_CALLEES` | No false positives |
| Pending native extra hint | `install_package="vertebra-core[oracle]"` (ch01 resolvers) | Accepted: gate checks the bracketed distribution name, not the bracketed extra | ch01 resolvers stay green until native flows declare `oracle` / `mssql` |
| SAQ factory, module absent | `import_optional` patched to `None`; `create_saq_plugin(redis_url="redis://localhost:6379")` | `package == "litestar-saq"`; `install_package == "vertebra-core[saq]"` | Message names the package, not a sentence |
| Gate ordering | gate test lands in `t04` | Green on arrival because `t01`–`t03` clear the twelve non-queue violations and `t04` clears the six queue literals | Suite never red between tasks |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given `MissingDependencyError("x", extra="sqlspec")`, when `hint` is read, then it equals `Install it with: uv add vertebra-core[sqlspec]`.
- Given a source file containing `ensure_module("wasmtime", extra="wasm")` and `wasm` absent from the declared extras, when `_optional_dependency_violations` scans it, then it returns `["pkg/mod.py:1::ensure_module extra='wasm'"]`.
- Given the repository after this chapter, when `test_optional_dependency_literals_name_declared_extras` runs, then the violation list is `[]`.
- Given `asyncpg` and `psycopg_pool` absent, when `AsyncPostgreSQLDriver(PostgreSQLConfig(dsn="postgresql://unreachable-host:5432/db")).execute_tuples("SELECT 1")` runs, then `MissingDependencyError.package == "psycopg_pool"` and `install_package == "psycopg[binary,pool]"`.
- Given `asyncpg` importable, `psycopg_pool` absent, and `SyncPostgreSQLDriver._run_sync_query` patched to raise `AssertionError`, when the same call runs, then `MissingDependencyError` with `install_package == "psycopg[binary,pool]"` is raised and the patched method is never called.
- Given `litestar_mcp` unresolvable through `import_optional`, when `create_mcp_plugin(name="Vertebra Agent MCP")` runs, then `MissingDependencyError.package == "litestar-mcp"` and `install_package == "vertebra-core[mcp]"`.

### 2.6 Alternatives Considered

| Alternative | Depth / Locality / Seam | Outcome |
| --- | --- | --- |
| Declare interim `postgres`, `mysql`, `wasm`, `numpy`, `pandas` extras | Keeps call sites unchanged; extras collide with native wheel extras owned by the native driver flows | Rejected: root non-goal; hints name pip packages directly |
| Regex grep gate over source text | Shallow; misses multi-line calls and attribute callees such as `module_loader.ensure_module(...)`, false-positives on logging `extra=` | Rejected: AST walk on `ast.Call` with callee filter and literal check |
| Gate also requires bracketed extras in `install_package` to be declared | Strict truthfulness; flags ch01's `vertebra-core[oracle]` / `vertebra-core[mssql]` until native flows ship | Rejected for this chapter: gate checks the bracketed distribution name; the extra-declared rule applies to `extra=` per R11 |
| Keep `asyncpg` probe in `_ensure_async_pool` and only fix the package name | Guard passes when `asyncpg` is present, then the thread-offload path raises for `psycopg_pool` | Rejected: the guard must check the module the guarded path imports |
| One task for all eight sentence literals plus the gate | Four production files in one task, over the three-file task bound | Rejected: SAQ and MCP literals land in `t03`, queues literals and the gate in `t04` |
| Land the gate first and fix literals after | Gate test red until the last literal task commits | Rejected: the gate lands in the task that clears the final violations |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch08]`
- Install-hint prose in `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` is owned by [ch11-docs-and-okf](../ch11-docs-and-okf/spec.md); this chapter changes no user docs. Python docstrings on touched helpers follow Google style.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R10 (hint default, loader, Litestar plugin) | [t01](tasks/t01-missing-dependency-hint-default.md) | `behavior_tdd` | `src/py/tests/unit/framework/test_typing.py`, `src/py/tests/unit/framework/test_framework_subdomains.py` |
| R10 (engine and WASM hints) | [t02](tasks/t02-engine-and-wasm-install-hints.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/tests/unit/db/test_mysql_driver.py`, `src/py/tests/unit/extensions/test_grpc_service.py` |
| R10 (SAQ and MCP package names) | [t03](tasks/t03-saq-and-mcp-package-names.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_litestar_ecosystem_frontend_and_saq.py`, `src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py` |
| R10 (queues package names), R11 | [t04](tasks/t04-queues-package-names-and-extras-gate.md) | `behavior_tdd` | `src/py/tests/unit/build/test_packaging_and_pgo.py`, `src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py` |

## 5. Implementation Plan

- [x] [9a60db2c] driver-fail-loud:ch08:t01 — `MissingDependencyError` defaults to `vertebra-core[<extra>]`; loader and Litestar plugin hints name real packages (tasks/t01-missing-dependency-hint-default.md)
- [x] [9a60db2c] driver-fail-loud:ch08:t02 — PostgreSQL, MySQL, and WASM guards name pip packages without undeclared extras; async PostgreSQL guard checks `psycopg_pool` (tasks/t02-engine-and-wasm-install-hints.md)
- [x] [9a60db2c] driver-fail-loud:ch08:t03 — `create_saq_plugin` and `create_mcp_plugin` pass `litestar-saq` / `litestar-mcp` as `package=` (tasks/t03-saq-and-mcp-package-names.md)
- [x] [9a60db2c] driver-fail-loud:ch08:t04 — litestar-queues helpers and SHM backends pass `litestar-queues`; AST gate rejects undeclared extras and malformed package literals (tasks/t04-queues-package-names-and-extras-gate.md)
- **Chapter Recovery Checkpoint**: `make lint && uv run --no-sync pytest src/py/tests/unit/framework/test_typing.py src/py/tests/unit/framework/test_framework_subdomains.py src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_mysql_driver.py src/py/tests/unit/extensions/test_grpc_service.py src/py/tests/unit/build/test_packaging_and_pgo.py src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py src/py/tests/unit/extensions/test_litestar_ecosystem_frontend_and_saq.py src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `9a60db2c` — chapter completed; `4/4` tasks completed.
- **Remaining Tasks**: none.
- **Documentation & OKF Sync**: none owned; `ch11-docs-and-okf` documents install hints.
- **Active Invariants**: no engine extras in `pyproject.toml`; `install_package` overrides still win over `extra`; the gate stays green after every task from `t04` on; `test_engine_driver_resolution_and_provide_driver` stays owned by ch01 / ch02.

[^missing-dep]: `src/py/vertebra/_typing.py::MissingDependencyError` — Missing dependency error and vertebra-core[extra] install hint default.
[^module-loader]: `src/py/vertebra/utils/module_loader.py::ensure_module` — Optional module loader and ensure_numpy / ensure_pandas helpers.
[^litestar-plugin]: `src/py/vertebra/extensions/litestar/plugin.py::VertebraPlugin` — Litestar plugin raising MissingDependencyError with vertebra-core[mcp].
[^postgres-driver]: `src/py/vertebra/db/adapters/postgres/driver.py::AsyncPostgreSQLDriver` — PostgreSQL _ensure_pool and _ensure_async_pool psycopg[binary,pool] guards.
[^mysql-driver]: `src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver` — MySQL pymysql and asyncmy dependency guards.
[^wasm-host]: `src/py/vertebra/wasm/__init__.py::WASMPluginHost` — WASM plugin host wasmtime dependency guard.
[^queues-config]: `src/py/vertebra/extensions/litestar/queues/config.py::create_sqlspec_queue_config` — litestar-queues config helpers naming litestar-queues as package.
[^queues-backend]: `src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend` — litestar-queues SHM backends naming litestar-queues as package.
[^saq-plugin]: `src/py/vertebra/extensions/litestar/saq/plugin.py::create_saq_plugin` — litestar-saq plugin factory naming litestar-saq as package.
[^mcp-plugin]: `src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin` — litestar-mcp plugin factory naming litestar-mcp as package.
[^pyproject]: `pyproject.toml` — Declared project optional-dependencies.
[^packaging-test]: `src/py/tests/unit/build/test_packaging_and_pgo.py` — Packaging and extras assertions.
[^saq-tests]: `src/py/tests/unit/extensions/test_litestar_ecosystem_frontend_and_saq.py::test_missing_dependency_errors` — SAQ missing-dependency test extended with a create_saq_plugin assertion.
[^mcp-tests]: `src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py::test_missing_dependency_error_on_missing_mcp` — MCP missing-dependency test extended with a create_mcp_plugin assertion.
