---
type: Task
id: "driver-fail-loud:ch11:t02"
chapter_id: ch11-docs-and-okf
title: Remove stand-in and TDS claims from reference pages and the analytics guide
description: Rewrite the vertebra.db reference overview and tenant paragraph, the API reference databases summary, and the analytics guide tenant and DuckDB sections to match the fail-loud driver baseline.
status: stable
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "driver-fail-loud:ch11:t01"
read_set:
  - docs/reference/db.md
  - docs/reference/index.md
  - docs/usage/analytics-and-cdc.md
  - src/py/vertebra/db/adapters/oracle/core.py
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/db/adapters/duckdb/driver.py
files:
  - docs/reference/db.md
  - docs/reference/index.md
  - docs/usage/analytics-and-cdc.md
tests: []
estimated_loc: 40
plan_revision: 1
plan_commit: 1e1f028a
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-08T00:00:00Z"
commit: null
sources:
  - id: docs-reference-db
    resource: docs/reference/db.md
    title: vertebra.db API reference page
  - id: docs-reference-index
    resource: docs/reference/index.md
    title: API reference landing page
  - id: docs-analytics
    resource: docs/usage/analytics-and-cdc.md
    title: Tenant predicates, CDC, and Arrow analytics guide
  - id: oracle-core
    resource: src/py/vertebra/db/adapters/oracle/core.py::OracleCore
    title: Oracle session context SQL builder
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

# Task t02: Reference Pages and Analytics Guide

## Objective & Scope Boundary

Rewrite three files so reference and analytics docs carry no SQLite stand-in or TDS framing claims and state engine status consistently with t01.[^docs-reference-db][^docs-reference-index][^docs-analytics]

In scope: `docs/reference/db.md` opening overview table and the closing tenant paragraph after `::: vertebra.db.rls.TenantPredicate`; the `docs/reference/index.md` "Databases" table row for "Database Drivers & Pools"; `docs/usage/analytics-and-cdc.md` "Tenant Predicates and Session Settings" Oracle paragraph and "Zero-Copy Arrow SHM (`ArrowCursor`), DuckDB & DataFusion" section.

Out of scope: `:::` mkdocstrings directives (rendered from docstrings owned by ch01, ch05, ch06, and ch07); CDC content; files owned by t01, t03, t04.

## Bounded Read Manifest

- `docs/reference/db.md` — overview table rows "SQLSpec Adapters (Litestar)" and "Relational & OLAP Configs"; tenant paragraph starting "`TenantScopeConfig.setting_name` is adapter-neutral".[^docs-reference-db]
- `docs/reference/index.md` — "Databases" table, "Database Drivers & Pools" row.[^docs-reference-index]
- `docs/usage/analytics-and-cdc.md` — paragraph starting "`OracleCore.build_session_context_sql()` validates"; DuckDB section and bullet "DuckDB `httpfs` Active ADC Rotation".[^docs-analytics]
- `src/py/vertebra/db/adapters/oracle/core.py::OracleCore.build_session_context_sql` — exact SQL and binds the builder returns.[^oracle-core]
- `src/py/vertebra/db/adapters/oracle/driver.py::_require_oracle_native` — guard behavior.[^oracle-driver]
- `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver` — `duckdb` guard.[^duckdb-driver]

## Symbol & Signature Contracts

- **db.md overview, "SQLSpec Adapters (Litestar)" row**: description keeps the config list and adds that Oracle and SQL Server sessions raise `MissingDependencyError` because their native driver packages are not yet published.
- **db.md overview, "Relational & OLAP Configs" row**: replace `TDS 7.4 @P1 rewriting` with `SQL Server @P1 placeholder rewriting`; replace `DuckDB Arrow streaming` with `DuckDB Arrow streaming (requires duckdb)`.
- **db.md tenant paragraph**: replace "Oracle context behavior currently has helper/stand-in acceptance; trusted-package and live-session acceptance remain separate driver work." with a present-tense statement: `OracleCore.build_session_context_sql()` returns the `DBMS_SESSION.SET_CONTEXT` block and named binds; Oracle drivers raise `MissingDependencyError`, so no Vertebra session executes it.
- **reference/index.md row**: engine list reads `PostgreSQL, MySQL, SQLite, DuckDB, BigQuery, Spanner, DataFusion, Redis, Valkey, and Cloud SQL / AlloyDB dialers`, plus `Oracle 26ai and SQL Server configurations (native drivers pending)`.
- **analytics-and-cdc.md Oracle paragraph**: delete "The current Oracle driver records these values in its SQLite stand-in. This verifies helper and emulation behavior only." and "belong to the native Oracle driver work"; state that Oracle drivers raise `MissingDependencyError`, keep the trusted-package / `CREATE CONTEXT` requirement and the Oracle docs link, keep "Vertebra does not provision database RLS or VPD policies."
- **analytics-and-cdc.md DuckDB section**: add one sentence before or after the code block that DuckDB execution requires the `duckdb` package and raises `MissingDependencyError` without it. Leave the code block unchanged.

## Edge Cases & Error Semantics

- Match t01 phrasing for engine status so README, usage, and reference pages agree: name `MissingDependencyError`, say the native Oracle and SQL Server packages are not yet published, and never quote a `vertebra-core[oracle]` / `[mssql]` install command.
- If `build_session_context_sql` returns a different SQL shape than the guide quotes, correct the guide to the code and add a Notes line.
- Leave `docs/changelog/index.md` untouched even though it names Oracle and DuckDB.
- No internal identifiers, sediment, or exclamation marks.

## Public-Seam Test Blueprint

Canonical gates: stale-claim grep, internal-name grep, relative-link check, `make docs-check`, `src/py/tests/unit/tools/test_docs_tooling.py`.

| Case | File / Section | Expected Text / Claim |
| --- | --- | --- |
| No stand-in | `docs/reference/db.md`, `docs/usage/analytics-and-cdc.md` | `grep -ciE 'stand-in\|emulation'` = `0` |
| No TDS | `docs/reference/db.md` overview | `grep -c 'TDS 7.4'` = `0`; contains `@P1` |
| Oracle tenant status | `docs/reference/db.md` tenant paragraph | Contains `build_session_context_sql` and `MissingDependencyError` |
| Analytics Oracle | `docs/usage/analytics-and-cdc.md` tenant section | Contains `MissingDependencyError`, `CREATE CONTEXT`, and the `DBMS_SESSION` docs link |
| Analytics DuckDB | `docs/usage/analytics-and-cdc.md` DuckDB section | Contains `duckdb` package requirement; code block byte-identical to baseline |
| Reference summary | `docs/reference/index.md` Databases row | Contains `native drivers pending` next to Oracle 26ai and SQL Server |
| No leaks | all three files | internal-name grep and planned-extra grep return no match |

## Verification

Baseline (expect matches before editing):

```bash
grep -nE 'stand-in|emulation|TDS 7\.4|native Oracle driver work' docs/reference/db.md docs/reference/index.md docs/usage/analytics-and-cdc.md
```

Final (all must hold):

```bash
! grep -niE 'stand-in|emulation|TDS 7\.4|native Oracle driver work' docs/reference/db.md docs/reference/index.md docs/usage/analytics-and-cdc.md
! grep -rnE 'native-[a-z-]+-drivers?|pending_flow_id|driver work|driver-fail-loud|ch[0-9]{2}' docs/reference/db.md docs/reference/index.md docs/usage/analytics-and-cdc.md
! grep -nE 'vertebra-core\[(oracle|mssql)\]' docs/reference/db.md docs/reference/index.md docs/usage/analytics-and-cdc.md
grep -n 'MissingDependencyError' docs/reference/db.md docs/usage/analytics-and-cdc.md
grep -n 'native drivers pending' docs/reference/index.md
uv run --no-sync python -I - docs/reference/db.md docs/reference/index.md docs/usage/analytics-and-cdc.md <<'PY'
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
make docs-check
uv run --no-sync pytest src/py/tests/unit/tools/test_docs_tooling.py -q
```

## Acceptance Criteria

- [ ] `docs/reference/db.md` and `docs/usage/analytics-and-cdc.md` contain no SQLite stand-in or emulation claims (R1, R13).
- [ ] `docs/reference/db.md` contains no TDS framing claim and states Oracle/SQL Server status (R8, R13).
- [ ] `docs/reference/index.md` marks Oracle 26ai and SQL Server native drivers pending (R13).
- [ ] `docs/usage/analytics-and-cdc.md` states the `duckdb` requirement (R3, R13).
- [ ] Every Verification command exits `0`.

## Notes & Discoveries

- [2026-11-07 00:00] Update `[k/11 files documented]` in the chapter spec and root spec when closing this task.

[^docs-reference-db]: `docs/reference/db.md` — vertebra.db API reference page.
[^docs-reference-index]: `docs/reference/index.md` — API reference landing page.
[^docs-analytics]: `docs/usage/analytics-and-cdc.md` — Tenant predicates, CDC, and Arrow analytics guide.
[^oracle-core]: `src/py/vertebra/db/adapters/oracle/core.py::OracleCore` — Oracle session context SQL builder.
[^oracle-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Oracle driver native-module guard.
[^duckdb-driver]: `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver` — DuckDB driver duckdb requirement.
