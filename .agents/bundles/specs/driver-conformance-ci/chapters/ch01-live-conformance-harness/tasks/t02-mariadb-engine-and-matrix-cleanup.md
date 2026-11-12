---
type: Task
id: "driver-conformance-ci:ch01:t02"
chapter_id: ch01-live-conformance-harness
title: Add the MariaDB conformance engine and remove dead conformance surface
description: Add a mariadb EngineConformanceSpec backed by vtb_mariadb_config, make MariaDB, SQL Server, and Oracle skip offline with explicit reasons, and delete _DUCKDB_SKIP_REASON, _FAIL_LOUD_DRIVER_ENGINES, the DuckDB env DSN declaration, and the duplicated SQL Server branch.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "driver-conformance-ci:ch01:t01"
read_set:
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/tests/integration/conftest.py
  - src/py/vertebra/db/adapters/mysql/config.py
  - .agents/bundles/specs/driver-conformance-ci/chapters/ch01-live-conformance-harness/spec.md
files:
  - src/py/tests/integration/db/test_driver_conformance.py
tests:
  - src/py/tests/integration/db/test_driver_conformance.py
estimated_loc: 120
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
commit: null
sources:
  - id: resolve-live
    resource: src/py/tests/integration/db/test_driver_conformance.py::_resolve_live_config
    title: Live configuration resolution per engine
  - id: engine-specs
    resource: src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS
    title: Conformance engine matrix
  - id: mysql-config
    resource: src/py/vertebra/db/adapters/mysql/config.py::MySQLConfig
    title: MySQL and MariaDB configuration and from_url parsing
tags:
  - conformance
  - mariadb
  - cleanup
---

# Task t02: Add the MariaDB Conformance Engine and Remove Dead Conformance Surface

## Objective & Scope Boundary

In `src/py/tests/integration/db/test_driver_conformance.py` [^resolve-live] [^engine-specs]:

1. Add `EngineConformanceSpec(engine="mariadb", config_cls=MySQLConfig, sync_driver_cls=SyncMySQLDriver, async_driver_cls=AsyncMySQLDriver, sample_url="mysql://vtb:secret@127.0.0.1:3307/vtb", placeholder_style="qmark", items_ddl=_MYSQL_ITEMS_DDL, types_ddl=_MYSQL_TYPES_DDL, supports_pipeline=True, native_extension_module="vertebra._mysql", env_dsn_vars=("VERTEBRA_CONFORMANCE_MARIADB_DSN",), container_fixture="vtb_mariadb_config")` directly after the `mysql` spec.
2. In `_resolve_live_config`, delete the dedicated `mssql` block and the `oracle`-only skip; after the generic env/container loop and the MySQL loopback branch, add `if spec.engine in _CONTAINER_ONLY_ENGINES: pytest.skip(f"{spec.engine} conformance requires {spec.env_dsn_vars[0]} or VERTEBRA_CONFORMANCE_DOCKER=1")` with module constant `_CONTAINER_ONLY_ENGINES: frozenset[str] = frozenset({"mariadb", "mssql", "oracle"})`. PostgreSQL resolution stays unchanged.
3. Delete `_DUCKDB_SKIP_REASON`, `_FAIL_LOUD_DRIVER_ENGINES` and its branch in `test_engine_driver_resolution_and_provide_driver`, and the DuckDB `env_dsn_vars=("VERTEBRA_CONFORMANCE_DUCKDB_DSN",)` argument; remove imports that become unused (`sys`, `reset_dependency_cache` when unused elsewhere in the module).
4. Add canonical regression tests for the new behavior.

Out of scope: PostgreSQL spec and resolution, gate module (`t01`), container fixtures (`t01`).

## Bounded Read Manifest

- `src/py/tests/integration/db/test_driver_conformance.py` — `ENGINE_SPECS`, `_resolve_live_config`, `_require_native_module`, `test_engine_driver_resolution_and_provide_driver`, `test_resolve_live_config_checks_native_module_before_credentials` [^resolve-live].
- `src/py/tests/integration/conftest.py` — `vtb_mariadb_config` (resolved by `t01`).
- `src/py/vertebra/db/adapters/mysql/config.py` — `MySQLConfig.from_url`, `MySQLConfig.pool_key` [^mysql-config].
- Chapter spec edge-case matrix.

## Symbol & Signature Contracts

- `_CONTAINER_ONLY_ENGINES: frozenset[str] = frozenset({"mariadb", "mssql", "oracle"})`.
- `_resolve_live_config(spec: EngineConformanceSpec, tmp_path: Path, request: pytest.FixtureRequest) -> BaseDatabaseConfig` keeps its signature; order: `_require_native_module(spec)` → sqlite/duckdb local paths → bigquery/spanner env gates → generic `env_dsn_vars` loop → container fixture under `VERTEBRA_CONFORMANCE_DOCKER == "1"` → MySQL loopback (engine `mysql` only) → `_CONTAINER_ONLY_ENGINES` skip → `spec.config_cls.from_url(spec.sample_url)`.
- The `mariadb` spec's parametrized id is `mariadb`, so `ConformanceGate.item_engines` resolves `frozenset({"mariadb"})` from the callspec.

## Edge Cases & Error Semantics

- MariaDB offline (no DSN, Docker unset): `pytest.skip("mariadb conformance requires VERTEBRA_CONFORMANCE_MARIADB_DSN or VERTEBRA_CONFORMANCE_DOCKER=1")`; no TCP connect to `127.0.0.1:3307`.
- `VERTEBRA_CONFORMANCE_MARIADB_DSN` set: config from DSN, no container.
- `vertebra._mysql` missing under Docker: `_require_native_module` fails the case (spec is not pending).
- SQL Server offline message: `"mssql conformance requires VERTEBRA_CONFORMANCE_MSSQL_DSN or VERTEBRA_CONFORMANCE_DOCKER=1"`.
- DuckDB keeps resolving to `tmp_path / "vtb_conformance.duckdb"` regardless of environment.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/integration/db/test_driver_conformance.py`. Dependency category: in-process (monkeypatched environment; no containers).

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| MariaDB URL contract | existing `test_engine_url_parsing_and_pool_key_contract[mariadb]` | `MySQLConfig` parsed; `pool_key()` tuple equals `DatabaseRegistry.pool_fingerprint("mysql://vtb:secret@127.0.0.1:3307/vtb")` |
| Container-only offline skips | parametrize `engine` over `("mariadb", "mssql", "oracle")`; `monkeypatch.delenv` the DSN var and `VERTEBRA_CONFORMANCE_DOCKER`; the test first calls `pytest.importorskip(spec.native_extension_module)` so the native-module gate passes | `pytest.skip.Exception` message equals `"<engine> conformance requires VERTEBRA_CONFORMANCE_<ENGINE>_DSN or VERTEBRA_CONFORMANCE_DOCKER=1"` |
| MariaDB DSN override | `VERTEBRA_CONFORMANCE_MARIADB_DSN="mysql://u:p@db.example:3306/app"` | Returns `MySQLConfig` with `host == "db.example"` and `port == 3306` without touching `request.getfixturevalue` |
| DuckDB ignores env DSN | `VERTEBRA_CONFORMANCE_DUCKDB_DSN="duckdb:////elsewhere.duckdb"` set | Returned `DuckDBConfig.resolved_path()` equals `str(tmp_path / "vtb_conformance.duckdb")` |
| Dead surface removed | `grep -n "_FAIL_LOUD_DRIVER_ENGINES\|_DUCKDB_SKIP_REASON" test_driver_conformance.py` | No matches |

## Verification

- Red: `uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py -k "mariadb or container_only" -q -rs` fails before the spec and tests exist.
- Green: `make dev && uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py -q -rs` exits `0` offline.
- Dead-surface check: `! git grep -n "_FAIL_LOUD_DRIVER_ENGINES\|_DUCKDB_SKIP_REASON\|VERTEBRA_CONFORMANCE_DUCKDB_DSN" -- src/py/tests/integration/db/test_driver_conformance.py`.
- Lint: `uv run --no-sync ruff check src/py/tests/integration/db/test_driver_conformance.py && uv run --no-sync ruff format --check src/py/tests/integration/db/test_driver_conformance.py`.

## Acceptance Criteria

- [ ] `mariadb` participates in every parametrized conformance case and resolves from DSN or `vtb_mariadb_config` (R4).
- [ ] MariaDB, SQL Server, and Oracle skip offline with the exact reasons; PostgreSQL resolution is unchanged (R4).
- [ ] `_DUCKDB_SKIP_REASON`, `_FAIL_LOUD_DRIVER_ENGINES`, the DuckDB DSN declaration, and the duplicated SQL Server branch are gone (R5).

## Notes & Discoveries

- [2026-11-12 00:30] The MySQL loopback branch stays for offline `mysql` runs; under `VERTEBRA_CONFORMANCE_DOCKER=1` the container branch precedes it, so live lanes never touch the loopback.

[^resolve-live]: `src/py/tests/integration/db/test_driver_conformance.py::_resolve_live_config` — Live configuration resolution per engine.
[^engine-specs]: `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` — Conformance engine matrix.
[^mysql-config]: `src/py/vertebra/db/adapters/mysql/config.py::MySQLConfig` — MySQL and MariaDB configuration.
