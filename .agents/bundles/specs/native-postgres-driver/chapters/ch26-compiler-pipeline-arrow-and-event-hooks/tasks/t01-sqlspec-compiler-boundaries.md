---
type: Task
id: "native-postgres-driver:ch26:t01"
chapter_id: ch26-compiler-pipeline-arrow-and-event-hooks
title: Prepare native PostgreSQL compiler boundaries without changing public defaults
description: Implement pure SQLSpec ParameterStyle.NUMERIC transport copy helpers and stage the SQLAlchemy numeric_dollar PostgreSQL compiler profile without altering legacy defaults before Chapter 27.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch25:t04"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
  - .agents/skills/python-framework/SKILL.md
files:
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
tests:
  - src/py/tests/unit/extensions/test_postgres_compiler_transport.py
estimated_loc: 230
plan_revision: 4
plan_commit: e8901de4
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-08T02:05:22Z"
commit: null
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: SQLSpec numeric transport copy and provenance preservation rules
  - id: sqlspec-py
    resource: src/py/vertebra/extensions/sqlspec/adapters.py::PostgreSQLSyncDriver
    title: SQLSpec PostgreSQLSyncDriver and PostgreSQLAsyncDriver execution styles
  - id: sqla-py
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncPostgreSQLDialect
    title: SQLAlchemy PostgreSQL +vtb/+vertebra DBAPI and dialect definitions
tags:
  - postgres
  - sqlspec
  - sqlalchemy
  - compiler
---

# Task t01: Prepare Native PostgreSQL Compiler Boundaries Without Changing Public Defaults

## Objective & Scope Boundary

Implement permanent SQLSpec `ParameterStyle.NUMERIC` prepared-statement transport copy helpers in `src/py/vertebra/extensions/sqlspec/adapters.py` and define/verify the PostgreSQL `numeric_dollar` SQLAlchemy DBAPI and compiler profile in `src/py/vertebra/extensions/sqlalchemy/dialects.py` while keeping registered `PostgreSQLSyncDriver._execution_parameter_style` (`POSITIONAL_PYFORMAT`) and `SyncPostgreSQLDialect` / `AsyncPostgreSQLDialect` defaults unchanged until atomic activation in `ch27:t02` [^npd-contracts] [^sqlspec-py] [^sqla-py].

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — SQLSpec prepared-statement provenance, single-execution statement transformers, and numeric transport boundaries [^npd-contracts].
- `src/py/vertebra/extensions/sqlspec/adapters.py` — `BaseSyncDriver`, `BaseAsyncDriver`, `PostgreSQLSyncDriver`, `PostgreSQLAsyncDriver`, `SQL`, and `StatementConfig` usage [^sqlspec-py].
- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — `SyncDBAPIModule`, `AsyncDBAPIModule`, `SyncPostgreSQLDialect`, and `AsyncPostgreSQLDialect` [^sqla-py].
- `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` — Existing SQLSpec and SQLAlchemy regression suite that must stay green prior to `ch27:t02`.
- [query-pipeline-cdc-and-tenancy.md](../../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md) — SQLSpec native statement compilation patterns.
- [SKILL.md](../../../../../../skills/python-framework/SKILL.md) — Typing and docstring conventions (zero inline `#` comments).

## Symbol & Signature Contracts

- `src/py/vertebra/extensions/sqlspec/adapters.py`:
  - Add permanent production helper(s) `_build_postgres_numeric_statement_config(base_config: StatementConfig | None = None) -> StatementConfig` and `_compile_postgres_numeric_transport(statement: SQL, driver_adapter: SyncDriverAdapterBase | AsyncDriverAdapterBase | None = None) -> tuple[str, Any]` (plus `execute_many` parameter-set variant):
    - Uses public `statement.copy(statement_config=...)` replacing `parameter_config` execution/output style with `ParameterStyle.NUMERIC` (`$1..$N`) and `prepare_driver_parameters` on the transport copy — lowering `?` (qmark), `:name` (colon named), `%s`, `%(name)s`, and `$N` inputs into numeric `$1..$N` SQL and ordered values.
    - Preserves original `statement` and `statement.statement_config` identity, attached filters, `TypedParameter` unwrapping/conversion, and `SQLResult.statement` / `StackResult.statement` provenance without mutating caller objects.
    - Clears or bypasses already-applied statement transformers on the copied already-prepared `SQL` instance so non-idempotent statement transformers execute exactly ONCE semantically.
    - Leaves `PostgreSQLSyncDriver._execution_parameter_style = ParameterStyle.POSITIONAL_PYFORMAT` untouched until `ch27:t02`.
- `src/py/vertebra/extensions/sqlalchemy/dialects.py`:
  - Support `paramstyle="numeric_dollar"` cleanly across `SyncDBAPIModule`, `AsyncDBAPIModule`, `SyncDBAPICursor`, `AsyncDBAPICursor`, and an explicit PostgreSQL `numeric_dollar` dialect configuration helper/constructor option (forwarding `$1..$N` SQL and positional sequence tuples directly to the native driver without `pyformat` `%(name)s` dict wrapping) while keeping default class attributes on `SyncPostgreSQLDialect` and `AsyncPostgreSQLDialect` unchanged until `ch27:t02`.

## Edge Cases & Error Semantics

- Never use regex rewriting on compiled SQL strings: SQLSpec's own lexer/parameter compiler and SQLAlchemy's `PGCompiler(paramstyle="numeric_dollar")` handle single quotes, `E'...'` strings, `$$`/`$tag$` dollar-quoted bodies, `--` and `/*...*/` comments, `::` type casts, and JSONB `?` / `?|` / `?&` operators natively.
- Repeated named `:id` or `%(id)s` parameters and out-of-order bindings map deterministically to ordered `$1..$N` positional values; SQLAlchemy `bindparam("ids", expanding=True)` `IN (:ids)` and `executemany` lower cleanly to `$1, $2, ...` sets.
- Do not introduce a runtime feature flag or dual-engine switch; `ch27:t02` wires these exact helpers as the unconditional PostgreSQL execution path.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/extensions/test_postgres_compiler_transport.py` (using real `sqlspec.SQL`, `StatementConfig`, `TypedParameter`, and real SQLAlchemy `select`/`insert`/`update`/`bindparam` compilers against `postgres_ports.py`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| SQLSpec `?`, `:name`, `%s`, `%(name)s`, & `$N` -> `$N` transport copy | `SQL("SELECT * FROM docs WHERE org = :org AND meta ?? 'k' AND id = :org", {"org": "acme"})` (and qmark `?` / pyformat / `TypedParameter` variants) | Original `SQL` retains caller `StatementConfig` identity and original compiled text; numeric transport copy emits `"SELECT * FROM docs WHERE org = $1 AND meta ? 'k' AND id = $1"` (preserving casts, quotes, dollar-quotes, comments, and JSONB operators) + ordered `("acme",)`. |
| Non-idempotent SQLSpec transformer single-application | Attach stateful transformer appending `" /* tx_once */"` to `StatementConfig`, prepare `SQL`, then invoke `_compile_postgres_numeric_transport` | Emitted numeric SQL contains `" /* tx_once */"` exactly ONCE (never duplicated). |
| `execute_many`, filters, Arrow, & `StatementStack` provenance | `SQL` with `where` filter + multi-row parameter sets + `SQLResult`/`StackResult` construction | Filters apply once; parameter sets lower to ordered tuples matching `$1..$N`; `SQLResult.statement is original_stmt`. |
| SQLAlchemy `numeric_dollar` profile (`SELECT`, `INSERT RETURNING`, expanding `IN`, `executemany`) | Compile and execute `select(t).where(t.c.id.in_(bindparam("ids", expanding=True)), t.c.tag == bindparam("tag"))` and `insert(t).returning(t.c.id)` via `numeric_dollar` profile against native port | Emits `$1, $2, $3` SQL and ordered tuple `(10, 20, "v1")` directly to native port; `executemany` passes sequence of ordered tuples. |
| Pre-activation legacy default preservation | Run `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` | Legacy `PostgreSQLSyncDriver` still uses `POSITIONAL_PYFORMAT` and existing SQLAlchemy `+vtb` tests stay 100% green. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/extensions/test_postgres_compiler_transport.py -q`
- Final Green + Regression + Lint/Typecheck gate:
  ```bash
  uv run --no-sync pytest src/py/tests/unit/extensions/test_postgres_compiler_transport.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -q
  uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters.py src/py/vertebra/extensions/sqlalchemy/dialects.py src/py/tests/unit/extensions/test_postgres_compiler_transport.py
  uv run --no-sync mypy src/py/vertebra/extensions/sqlspec src/py/vertebra/extensions/sqlalchemy
  ```

## Acceptance Criteria

- [ ] `adapters.py` provides pure `ParameterStyle.NUMERIC` transport copy helpers preserving caller `StatementConfig` identity, `SQLResult.statement` provenance, filters, and single-pass transformer semantics without regex rewrites (R2, R24).
- [ ] `dialects.py` supports the PostgreSQL `numeric_dollar` DBAPI/compiler profile across `SELECT`, `INSERT ... RETURNING`, `UPDATE`, expanding `IN`, and `executemany` while keeping pre-`ch27` registered defaults green (R2, R24).
- [ ] Commit message follows `feat(postgres): prepare native consumer transport compilation` once verified.

## Notes & Discoveries

- Note the `ch26:t01` -> `ch27:t02` contract: `ch27:t02` activates these exact helpers on `PostgreSQLSyncDriver`, `PostgreSQLAsyncDriver`, `SyncPostgreSQLDialect`, and `AsyncPostgreSQLDialect` and switches `test_postgres_compiler_transport.py` to exercise canonical public adapter methods [^npd-contracts].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — SQLSpec numeric transport copy and provenance preservation rules.
[^sqlspec-py]: `src/py/vertebra/extensions/sqlspec/adapters.py::PostgreSQLSyncDriver` — SQLSpec PostgreSQLSyncDriver and PostgreSQLAsyncDriver execution styles.
[^sqla-py]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncPostgreSQLDialect` — SQLAlchemy PostgreSQL +vtb/+vertebra DBAPI and dialect definitions.
