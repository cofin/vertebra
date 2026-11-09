---
type: Task
id: "driver-fail-loud:ch11:t01"
chapter_id: ch11-docs-and-okf
title: State engine status, DuckDB requirement, memory:// scope, and the vertebra-db role in README and usage guides
description: Rewrite the README driver list and the databases and extensions usage guides so Oracle and SQL Server read as pending native drivers, DuckDB requires duckdb, which free-threaded interpreters do not install, engine drivers reject memory://, vertebra-db reads as codecs and transport, and unparsable +vtb URLs raise InterfaceError.
status: stable
state: completed
priority: P2
verification_strategy: documentation_validation
depends_on: []
read_set:
  - README.md
  - docs/usage/databases.md
  - docs/usage/extensions.md
  - src/py/vertebra/_typing.py
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/db/adapters/duckdb/driver.py
files:
  - README.md
  - docs/usage/databases.md
  - docs/usage/extensions.md
tests: []
estimated_loc: 75
plan_revision: 2
plan_commit: c2b09b9b
state_revision: 1
claimed_by: agent:flow-executor
claimed_at: "2026-11-09T15:05:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T15:11:12Z"
last_verified_commit: 30bc516e
verification_evidence: "stale-claim grep, relative-link check, make docs, and pytest src/py/tests/unit/tools/test_docs_tooling.py -q (28 passed)"
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-09T15:11:12Z"
commit: 30bc516e
sources:
  - id: readme
    resource: README.md
    title: Repository README driver list
  - id: docs-databases
    resource: docs/usage/databases.md
    title: Databases and native drivers usage guide
  - id: docs-extensions
    resource: docs/usage/extensions.md
    title: SQLSpec and SQLAlchemy extension guide
  - id: missing-dep
    resource: src/py/vertebra/_typing.py::MissingDependencyError
    title: Missing dependency error and install hint
  - id: oracle-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: Oracle driver native-module guard
  - id: duckdb-driver
    resource: src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver
    title: DuckDB driver duckdb requirement
tags:
  - docs
  - database
  - fail-loud
---

# Task t01: README and Usage Guides

## Objective & Scope Boundary

Rewrite three user-facing files so they describe the post-ch09 driver behavior.[^readme][^docs-databases][^docs-extensions]

In scope: the README "Storage, Databases & TypeScript Client" bullet; `docs/usage/databases.md` intro paragraph, mermaid diagram, "Optional Engine Extras", and "Native Database Drivers & Adapters Matrix"; `docs/usage/extensions.md` "SQLSpec Integration, Statement Stacks & SHM Proxy" and "SQLAlchemy 2.1, Advanced Alchemy 1.12 & `+vtb` Dialects" sections.

Out of scope: reference pages and the analytics guide (t02); OKF files (t03, t04); queue, testing, and object-storage `memory://` examples; `docs/changelog/index.md`; adding new extras to install tabs.

## Bounded Read Manifest

- `README.md` — "Storage, Databases & TypeScript Client" bullet under the intro feature list.[^readme]
- `docs/usage/databases.md` — intro paragraph under the H1, mermaid block, "Optional Engine Extras", "Native Database Drivers & Adapters Matrix".[^docs-databases]
- `docs/usage/extensions.md` — "SQLSpec Integration, Statement Stacks & SHM Proxy (`vertebra.extensions.sqlspec`)", "SQLAlchemy 2.1, Advanced Alchemy 1.12 & `+vtb` Dialects".[^docs-extensions]
- `src/py/vertebra/_typing.py::MissingDependencyError` — exact message and hint text.[^missing-dep]
- `src/py/vertebra/db/adapters/oracle/driver.py::_require_oracle_native` — module name and error the Oracle guard raises (MSSQL mirrors it with `vertebra._mssql`). Its hint names the planned `vertebra-core[oracle]` extra, which is not published; read it to confirm the guard, never to copy the command into docs.[^oracle-driver]
- `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver` — `duckdb` import guard and hint.[^duckdb-driver]

## Symbol & Signature Contracts

Target text, per section (adapt wording; keep every fact):

- **README bullet**: database drivers list reads `Postgres, MySQL, SQLite, DuckDB, Google BigQuery, Google Cloud Spanner, Redis, Valkey, DataFusion`, followed by a clause that Oracle 26ai and SQL Server configs ship while their native drivers are pending.
- **databases.md intro paragraph**: replace "with native Rust shared-memory database drivers (`vertebra.db` / `vertebra-db`)" with a statement that SQLSpec adapters sit on `vertebra.db` engine drivers; each engine's driver lives in `vertebra.db.adapters.<engine>`, BigQuery and Spanner run on their own native wheels, the other engines use their Python client packages, and the Rust `vertebra-db` crate supplies shared wire codecs, Cloud SQL / AlloyDB dialers, and shared-memory Arrow transport. Keep the lazy-import sentence and the three guide links.
- **databases.md mermaid**: node `D` reads `Engine Drivers & vertebra-db Codecs` (no "Native Drivers (Rust SHM)"); the engine node lists `PostgreSQL / MySQL / SQLite / DuckDB / BigQuery / Spanner`. Keep `4–6` nodes.
- **databases.md "Optional Engine Extras"**: after the install tabs, add two short paragraphs:
  - DuckDB requires the `duckdb` package (`uv add duckdb`); without it `SyncDuckDBDriver` and `AsyncDuckDBDriver` raise `MissingDependencyError`. DuckDB publishes no free-threaded wheel, so `duckdb` is not installed on Python `3.14t` / `3.15t` and DuckDB drivers raise `MissingDependencyError` there.
  - The native Oracle and SQL Server driver packages are not yet published. `OracleConfig` and `MSSQLConfig` import and parse URLs; constructing `SyncOracleDriver`, `AsyncOracleDriver`, `SyncMSSQLDriver`, or `AsyncMSSQLDriver` raises `MissingDependencyError`. Name the error only: do not quote its install hint or any `uv add vertebra-core[oracle]` / `vertebra-core[mssql]` command, because those extras are not installable.
- **databases.md matrix**:
  - Intro sentence keeps the engine count and adds that Oracle and SQL Server rows describe configuration and codecs only.
  - Oracle row "Key Capabilities": `Native driver pending (drivers raise MissingDependencyError); Autonomous Database wallet and tnsnames.ora parsing, 26ai VECTOR and OSON codecs`. Remove `Pure-Rust Thin TNS/TTC wire framing`.
  - SQL Server row: `Native driver pending (drivers raise MissingDependencyError); ? / $1 / :name to @P1 parameter rewriting`. Remove `TDS 7.4/8.0 framing` and `ArrowCursor`.
  - DuckDB row: prefix `Requires the duckdb package (not installed on 3.14t / 3.15t, where DuckDB publishes no free-threaded wheel and the drivers raise MissingDependencyError);`.
  - After the table add one paragraph: `memory://` is not an engine URL. PostgreSQL, MySQL, Redis, and Valkey configs still construct from it, but their drivers raise `ImproperConfigurationError`, and so do the `from_valkey` / `from_redis` factories of `KeyValueStore`, the Litestar `MemoryStore`, the Starlette `CacheStore`, and `SQLAlchemyCacheBackend`. `DatabaseRegistry.parse_url("memory://")` raises `ValueError`. `QueueConfig(backend_url="memory://")`, `SyncObjectStore("memory://")`, and `SQLAlchemyCacheBackend.from_url("memory://")` keep their in-process backends.
- **extensions.md SQLSpec section**: "across 8 database engines" sentence adds that Oracle and SQL Server configs construct but session acquisition raises `MissingDependencyError`.
- **extensions.md SQLAlchemy "Dialects" bullet**: replace "across all 8 engines" with the explicit scheme list it already prints, state that `oracle+vtb://` and `mssql+vtb://` connections raise `MissingDependencyError`, that `duckdb+vtb://` requires the `duckdb` package, and that a URL `DatabaseRegistry.parse_url` cannot parse raises the DB-API `InterfaceError`.

## Edge Cases & Error Semantics

- The post-ch01 guard hint reads `Install it with: uv add vertebra-core[oracle]` / `[mssql]` and names planned extras absent from `[project.optional-dependencies]`. User docs never quote it. If the guard raises something other than `MissingDependencyError`, stop and report a contradiction.
- If ch02 left a DuckDB path that does not raise `MissingDependencyError`, stop and report a contradiction instead of documenting it.
- Keep `SyncObjectStore("memory://")` in the extensions.md Advanced Alchemy example and `sqlite:///:memory:` / `sqlite+vtb_async:///:memory:` URLs unchanged.
- If `src/py/vertebra/db/adapters/redis/driver.py` has no `_reject_memory_url` (ch04 t02 incomplete), stop and set `blocked_reason` instead of documenting the Redis/Valkey half of the `memory://` paragraph.
- No flow, chapter, or spec identifiers, no `Previously` / `now` sediment, no exclamation marks.
- No new fenced code blocks. If a code block is edited, execute it as described in Verification.

## Public-Seam Test Blueprint

Canonical gates: stale-claim grep, internal-name grep, relative-link check, `make docs`, `src/py/tests/unit/tools/test_docs_tooling.py`.

| Case | File / Section | Expected Text / Claim |
| --- | --- | --- |
| README status | `README.md` storage/databases bullet | Contains `DuckDB` and a pending clause naming `Oracle 26ai` and `SQL Server`; no bare `Oracle 26ai, SQL Server, DuckDB` run |
| vertebra-db role | `docs/usage/databases.md` intro + mermaid | `grep -c 'native Rust shared-memory database drivers'` = `0`; `grep -c 'Native Drivers (Rust SHM)'` = `0`; intro names codecs and dialers |
| Diagram engines | `docs/usage/databases.md` mermaid | Engine node has no `Oracle` or `MSSQL` |
| DuckDB requirement | `docs/usage/databases.md` extras + DuckDB row | Both contain `duckdb` package requirement; extras paragraph contains `MissingDependencyError`, `3.14t`, and `3.15t` |
| Oracle row | `docs/usage/databases.md` matrix | Contains `MissingDependencyError`; `grep -c 'Thin TNS/TTC'` = `0` |
| SQL Server row | `docs/usage/databases.md` matrix | Contains `@P1`; `grep -c 'TDS 7.4'` = `0` |
| No planned-extra command | all three files | `grep -cE 'vertebra-core\[(oracle\|mssql)\]'` = `0`; Oracle/SQL Server text says "not yet published" |
| memory:// scope | `docs/usage/databases.md` after matrix | Names PostgreSQL, MySQL, Redis, Valkey, `ImproperConfigurationError`, `from_valkey`, `KeyValueStore`, `CacheStore`, `SQLAlchemyCacheBackend`, `ValueError`, `QueueConfig`, `SyncObjectStore`, and `from_url("memory://")` |
| SQLSpec status | `docs/usage/extensions.md` SQLSpec section | Contains `MissingDependencyError` next to Oracle and SQL Server |
| Dialect claims | `docs/usage/extensions.md` Dialects bullet | `grep -c 'all 8 engines'` = `0`; contains `InterfaceError` and `duckdb+vtb://` next to the `duckdb` package |
| Kept examples | `docs/usage/extensions.md` Advanced Alchemy example | `SyncObjectStore("memory://")` still present |
| No leaks | all three files | internal-name grep returns no match |

## Verification

Baseline (expect matches before editing):

```bash
grep -nE 'Thin TNS/TTC|TDS 7\.4|all 8 engines|Oracle / MySQL / MSSQL|Oracle 26ai, SQL Server, DuckDB|native Rust shared-memory database drivers' README.md docs/usage/databases.md docs/usage/extensions.md
grep -n '_reject_memory_url' src/py/vertebra/db/adapters/redis/driver.py
```

The second command must match before editing (ch04 t02 complete); if it does not, stop and set `blocked_reason`.

Final (all must hold):

```bash
! grep -nE 'Thin TNS/TTC|TDS 7\.4|all 8 engines|Oracle / MySQL / MSSQL|Oracle 26ai, SQL Server, DuckDB|stand-in|native Rust shared-memory database drivers|Native Drivers \(Rust SHM\)' README.md docs/usage/databases.md docs/usage/extensions.md
! grep -nE 'vertebra-core\[(oracle|mssql)\]' README.md docs/usage/databases.md docs/usage/extensions.md
! grep -rnE 'native-[a-z-]+-drivers?|pending_flow_id|driver work|driver-fail-loud|ch[0-9]{2}' README.md docs/usage/databases.md docs/usage/extensions.md
grep -c 'MissingDependencyError' docs/usage/databases.md docs/usage/extensions.md
grep -n 'not yet published' docs/usage/databases.md
grep -nE '3\.14t' docs/usage/databases.md
grep -n 'SQLAlchemyCacheBackend.from_url("memory://")' docs/usage/databases.md
grep -n 'duckdb+vtb://' docs/usage/extensions.md
grep -n 'SyncObjectStore("memory://")' docs/usage/extensions.md
uv run --no-sync python -I - README.md docs/usage/databases.md docs/usage/extensions.md <<'PY'
import pathlib, re, sys
bad = []
for p in map(pathlib.Path, sys.argv[1:]):
    for target in re.findall(r"\]\(([^)#\s]+)(?:#[^)]*)?\)", p.read_text()):
        if "://" in target or target.startswith("mailto:"):
            continue
        if not (p.parent / target).resolve().exists():
            bad.append(f"{p}: {target}")
print("\n".join(bad) or "all relative links resolve")
sys.exit(1 if bad else 0)
PY
make docs
uv run --no-sync pytest src/py/tests/unit/tools/test_docs_tooling.py -q
```

If any fenced code block changed, save it to a scratch file and run `uv run --no-sync python -I <scratch-file>`; it must exit `0` or raise only the documented `MissingDependencyError`.

## Acceptance Criteria

- [x] README and `docs/usage/databases.md` mark Oracle and SQL Server as pending native drivers raising `MissingDependencyError`, state that their native packages are not yet published, and quote no `vertebra-core[oracle]` / `[mssql]` command (R13, R1).
- [x] `docs/usage/databases.md` intro and mermaid describe `vertebra-db` as shared codecs, dialers, and transport (R13, R8).
- [x] `docs/usage/databases.md` states the `duckdb` package requirement and that DuckDB publishes no free-threaded wheel, so `duckdb` is not installed on `3.14t` / `3.15t` and DuckDB drivers raise `MissingDependencyError` there (R13, R3).
- [x] `docs/usage/databases.md` states that PostgreSQL/MySQL/Redis/Valkey drivers and the Redis/Valkey store factories raise `ImproperConfigurationError` for `memory://`, that `parse_url("memory://")` raises `ValueError`, and that queue, object-storage, and `SQLAlchemyCacheBackend.from_url` `memory://` stay (R6, R7).
- [x] `docs/usage/extensions.md` dialect and SQLSpec claims are accurate, including `InterfaceError` for unparsable URLs and the `duckdb` requirement for `duckdb+vtb://` (R3, R4, R13).
- [x] Every Verification command exits `0`.

## Notes & Discoveries

- [2026-11-07 00:00] Update `[k/11 files documented]` in the chapter spec and root spec when closing this task.

[^readme]: `README.md` — Repository README driver list.
[^docs-databases]: `docs/usage/databases.md` — Databases and native drivers usage guide.
[^docs-extensions]: `docs/usage/extensions.md` — SQLSpec and SQLAlchemy extension guide.
[^missing-dep]: `src/py/vertebra/_typing.py::MissingDependencyError` — Missing dependency error and install hint.
[^oracle-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Oracle driver native-module guard.
[^duckdb-driver]: `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver` — DuckDB driver duckdb requirement.
