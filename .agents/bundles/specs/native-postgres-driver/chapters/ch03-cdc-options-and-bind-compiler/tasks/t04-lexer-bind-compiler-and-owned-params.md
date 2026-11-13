---
type: Task
id: "native-postgres-driver:ch03-cdc-options-and-bind-compiler:t04"
flow_id: native-postgres-driver
chapter_id: ch03-cdc-options-and-bind-compiler
title: "Implement single-pass lexer binds.py and owned parameter normalization"
description: "Compile %s, %(name)s, and $N binds in one lexical pass while normalizing mutable buffers, JSONB text, and pgvector inputs."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch03:t03"
files: 
  - src/py/vertebra/db/adapters/postgres/binds.py
  - src/py/vertebra/db/adapters/postgres/core.py
tests: 
  - src/py/tests/unit/db/test_postgres_options_and_binds.py
read_set:
  - src/py/vertebra/db/adapters/postgres/core.py
  - src/py/vertebra/db/adapters/postgres/types.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
estimated_loc: 225
tags:
  - python
  - postgres
  - parser
sources:
  - id: pg-core
    resource: "src/py/vertebra/db/adapters/postgres/core.py::PostgreSQLCore"
    title: "PostgreSQL SQL and parameter helpers"
---

# Task `t04`: Implement single-pass lexer binds.py and owned parameter normalization

## Objective & Scope Boundary

Implement `binds.py` and wire `PostgreSQLCore` bind helpers per `../spec.md` §2 [^pg-core].

## Bounded Read Manifest

- `postgres/core.py`, `postgres/types.py`, and `contracts.md`.

## Contracts & Edge Cases

- Single-pass state machine skipping `'...'`, `E'...'` (`\'`), `"..."`, `$tag$...$tag$`, `--...`, `/*...*/` (nested), `::` casts, and `?`/`?|`/`?&`; reject mixed `%s` and `%(name)s` (`ProgrammingError`).
- Copy mutable `bytearray`/`memoryview` inputs into owned `bytes` before FFI detach so caller mutation after call start never corrupts wire frames.

## Test Matrix

- Parameterized lexer edge-case matrix (`E'...'`, nested block comments, `$tag$`, `jsonb ?|`, `%%` escapes, mixed-style errors, mutable buffer isolation).

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_options_and_binds.py -q && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres
```

## Acceptance Criteria

- [ ] Lexer preserves all SQL literals/operators and `mypy` passes across `db/adapters/postgres`.
- [ ] Zero regressions in `ch03-cdc-options-and-bind-compiler` fast verification gate.

[^pg-core]: `src/py/vertebra/db/adapters/postgres/core.py::PostgreSQLCore` — PostgreSQL SQL and parameter helpers.
