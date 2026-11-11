---
type: Task
id: "native-postgres-driver:ch26:t01"
chapter_id: ch26-compiler-pipeline-arrow-and-event-hooks
title: Prepare native PostgreSQL compiler boundaries without changing public defaults
description: Implement pure sqlspec==0.71.1 ParameterStyle.NUMERIC transport copy helpers preserving statement_transformers and unwrapping TypedParameter, and stage the SQLAlchemy numeric_dollar PostgreSQL profile without altering pre-cutover defaults.
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
estimated_loc: 235
plan_revision: 6
plan_commit: 043d8444
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
updated_at: "2026-11-10T14:45:00Z"
commit: null
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: 043d8444
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: SQLSpec numeric transport copy, TypedParameter unwrapping, and SQLAlchemy numeric_dollar rules
  - id: sqlspec-py
    resource: src/py/vertebra/extensions/sqlspec/adapters.py::PostgreSQLSyncDriver
    title: SQLSpec PostgreSQLSyncDriver and PostgreSQLAsyncDriver execution styles and parameter preparation
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

Implement permanent `sqlspec==0.71.1` `ParameterStyle.NUMERIC` prepared-statement transport copy helpers in `src/py/vertebra/extensions/sqlspec/adapters.py` (preserving `base_config.statement_transformers`, avoiding `StatementFilter` re-application, compiling once, and unwrapping `TypedParameter` for both single and `is_many=True` binds) and stage the PostgreSQL `numeric_dollar` SQLAlchemy DBAPI and dialect profile (`default_paramstyle = "numeric_dollar"` and `paramstyle = "numeric_dollar"`) in `src/py/vertebra/extensions/sqlalchemy/dialects.py` while keeping registered `PostgreSQLSyncDriver._execution_parameter_style` (`POSITIONAL_PYFORMAT`) and `SyncPostgreSQLDialect` / `AsyncPostgreSQLDialect` defaults unchanged until atomic activation in `ch27:t02` [^npd-contracts] [^sqlspec-py] [^sqla-py].

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — SQLSpec prepared-statement provenance, single-execution `sqlglot` AST statement transformers, `TypedParameter` unwrapping, and SQLAlchemy offline+live `numeric_dollar` rules [^npd-contracts].
- `src/py/vertebra/extensions/sqlspec/adapters.py` — `BaseSyncDriver`, `BaseAsyncDriver`, `PostgreSQLSyncDriver`, `PostgreSQLAsyncDriver`, `SQL`, `TypedParameter`, `StatementConfig`, and `prepare_driver_parameters` [^sqlspec-py].
- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — `SyncDBAPIModule`, `AsyncDBAPIModule`, `SyncPostgreSQLDialect`, and `AsyncPostgreSQLDialect` [^sqla-py].
- `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` — Existing SQLSpec and SQLAlchemy regression suite kept green prior to `ch27:t02`.
- [query-pipeline-cdc-and-tenancy.md](../../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md) — SQLSpec native statement compilation patterns.
- [SKILL.md](../../../../../../skills/python-framework/SKILL.md) — Typing and docstring conventions (zero inline `#` comments).

## Symbol & Signature Contracts

- `src/py/vertebra/extensions/sqlspec/adapters.py`:
  - `_build_postgres_numeric_statement_config(base_config: StatementConfig | None = None) -> StatementConfig`:
    - Builds a derived `StatementConfig` setting `parameter_config` execution/output style to `ParameterStyle.NUMERIC` (`$1..$N`, preserving supported input styles) while **preserving `base_config.statement_transformers` untouched** (never clearing `statement_transformers=()`, which in `sqlspec==0.71.1` would strip caller `sqlglot` AST transformers from uncompiled `SQL` statements).
  - `_compile_postgres_numeric_transport(statement: SQL, driver_adapter: SyncDriverAdapterBase | AsyncDriverAdapterBase | None = None, *, is_many: bool = False) -> tuple[str, tuple[Any, ...] | list[tuple[Any, ...]]]`:
    - Creates an unmutated `transport_stmt = statement.copy(statement_config=_build_postgres_numeric_statement_config(statement.statement_config))` without re-applying already-lowered `StatementFilter` instances attached to `statement`, and compiles `transport_stmt` **exactly once** so `base_config.statement_transformers` run once total (`?`, `:name`, `%s`, `%(name)s`, and `$N` lower cleanly to `$1..$N`).
    - Preserves original `statement` and `statement.statement_config` identity (`SQLResult.statement is statement`) without mutating caller objects even when the caller pre-compiled `statement.compile()` before execution.
    - Invokes `prepare_driver_parameters(..., statement_config=transport_stmt.statement_config, is_many=is_many)` and explicitly unwraps `TypedParameter` (`val.value if isinstance(val, TypedParameter) else val`) across **both** single parameter sequences (`tuple(...)`) and `is_many=True` parameter sets (`[tuple(val.value if isinstance(val, TypedParameter) else val for val in row) for row in prepared_sets]`) so `TypedParameter` wrappers never cross into `vertebra._postgres`.
    - Leaves `PostgreSQLSyncDriver._execution_parameter_style = ParameterStyle.POSITIONAL_PYFORMAT` untouched until `ch27:t02`.
- `src/py/vertebra/extensions/sqlalchemy/dialects.py`:
  - Support `paramstyle="numeric_dollar"` cleanly across `SyncDBAPIModule(paramstyle="numeric_dollar")`, `AsyncDBAPIModule(paramstyle="numeric_dollar")`, `SyncDBAPICursor`, `AsyncDBAPICursor`, and an explicit staged PostgreSQL `numeric_dollar` dialect configuration that sets **both** `default_paramstyle = "numeric_dollar"` and `paramstyle = "numeric_dollar"` (matching the exact class attributes activated directly on `SyncPostgreSQLDialect` and `AsyncPostgreSQLDialect` in `ch27:t02`) so both offline `stmt.compile(dialect=...)` (with zero DBAPI connection) and live engine execution emit `$1..$N` SQL and positional parameter tuples without `pyformat` `%(name)s` dicts.

## Edge Cases & Error Semantics

- Never use regex rewriting on compiled SQL strings: SQLSpec's lexer/AST compiler and SQLAlchemy's `PGCompiler` (`numeric_dollar`) handle single quotes, `E'...'` strings, `$$`/`$tag$` dollar-quoted bodies, `--` and `/*...*/` comments, `::` type casts, and JSONB `?` / `?|` / `?&` operators natively.
- When a caller runs `statement.compile()` manually before passing `statement` to `_compile_postgres_numeric_transport`, construct the transport copy from `statement`'s raw SQL/AST source state (not already-transformed text) so `statement_transformers` still apply exactly once on the `$N` compilation pass and already-applied `StatementFilter`s are never appended twice.
- Do not introduce a runtime feature flag or dual-engine switch; `ch27:t02` wires these exact helpers and `default_paramstyle = paramstyle = "numeric_dollar"` attributes directly onto the canonical PostgreSQL classes.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/extensions/test_postgres_compiler_transport.py` (using real `sqlspec.SQL`, `StatementConfig`, `TypedParameter`, real `sqlglot.exp` transformers, and real SQLAlchemy 2.1 compilers/engines against `postgres_ports.py`, parameterized via `@pytest.mark.parametrize`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| SQLSpec `?`, `:name`, `%s`, `%(name)s`, `$N`, & `TypedParameter` (single + `is_many=True`) | `SQL` across all input styles + `TypedParameter(uuid_val, UUID, ...)` in single query and `is_many=True` batch rows | Original `SQL` keeps caller `StatementConfig` identity; transport copy emits `$1..$N` preserving quotes, dollar-quotes, comments, `::`, and JSONB `?`/`?|`/`?&`; single and `is_many=True` outputs unwrap every `TypedParameter` to raw `.value`. |
| Real `sqlglot` AST transformer single-application | Attach stateful `(expression, parameters) -> (expression, parameters)` transformer incrementing `counter += 1` and adding AST `WHERE is_deleted = FALSE` predicate to `StatementConfig`; test both fresh `SQL` and pre-compiled `SQL` | `counter == 1` per compiled transport (not `0`, not `2`), `is_deleted = FALSE` appears once in emitted `$N` SQL, and caller `SQL.statement_config.statement_transformers` stays unmutated. |
| `StatementFilter`, Arrow, & `StatementStack` provenance | `SQL` with lowered `SearchFilter`/`LimitOffsetFilter` + `is_many=True` rows + `SQLResult`/`StackResult` construction | Filters apply once without duplicate `WHERE`/`LIMIT` clauses; `SQLResult.statement is original_stmt`. |
| SQLAlchemy offline + live `numeric_dollar` (`default_paramstyle` + `paramstyle` + DBAPI) | Offline `str(stmt.compile(dialect=pg_numeric_dialect))` and live sync/async execution of `select(...in_(expanding bindparam))`, `insert().returning()`, `update()`, and `executemany` against `postgres_ports.py` | Both offline `stmt.compile(...)` and live cursor execution emit `$1, $2, $3` and forward ordered positional tuples directly to native port. |
| Pre-activation legacy default preservation | Run `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` | Legacy `PostgreSQLSyncDriver` and `SyncPostgreSQLDialect`/`AsyncPostgreSQLDialect` defaults stay untouched and green prior to `ch27:t02`. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/extensions/test_postgres_compiler_transport.py -q`
- Final Green + Regression + Lint/Typecheck gate:
  ```bash
  uv run --no-sync pytest src/py/tests/unit/extensions/test_postgres_compiler_transport.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -q
  uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters.py src/py/vertebra/extensions/sqlalchemy/dialects.py src/py/tests/unit/extensions/test_postgres_compiler_transport.py
  uv run --no-sync mypy src/py/vertebra/extensions/sqlspec src/py/vertebra/extensions/sqlalchemy
  ```

## Acceptance Criteria

- [ ] `adapters.py` preserves `base_config.statement_transformers` on unmutated `statement.copy(...)`, compiles the `ParameterStyle.NUMERIC` copy once without re-applying `StatementFilter`s, unwraps `TypedParameter` across single and `is_many=True` binds, and preserves `SQLResult.statement` provenance (R2, R24).
- [ ] `dialects.py` stages `default_paramstyle = "numeric_dollar"`, `paramstyle = "numeric_dollar"`, and `SyncDBAPIModule`/`AsyncDBAPIModule(paramstyle="numeric_dollar")` emitting `$1..$N` in both offline compilation and live execution while keeping pre-`ch27` defaults green (R2, R24).
- [ ] Commit message follows `feat(postgres): prepare native consumer transport compilation` once verified.

## Notes & Discoveries

- `ch27:t02` activates `_compile_postgres_numeric_transport` across every `PostgreSQLSyncDriver` and `PostgreSQLAsyncDriver` execution path and promotes `default_paramstyle = "numeric_dollar"` + `paramstyle = "numeric_dollar"` onto `SyncPostgreSQLDialect` and `AsyncPostgreSQLDialect` directly [^npd-contracts].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — SQLSpec numeric transport copy, TypedParameter unwrapping, and SQLAlchemy numeric_dollar rules.
[^sqlspec-py]: `src/py/vertebra/extensions/sqlspec/adapters.py::PostgreSQLSyncDriver` — SQLSpec PostgreSQLSyncDriver and PostgreSQLAsyncDriver execution styles and parameter preparation.
[^sqla-py]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncPostgreSQLDialect` — SQLAlchemy PostgreSQL +vtb/+vertebra DBAPI and dialect definitions.
