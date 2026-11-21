---
type: Task
id: "native-cursor-data-path:network:t02"
chapter_id: network-engines
title: "Move binds.py onto the Rust bind plan and delete the Python scanner"
description: "compile_postgres_binds calls the Rust bind plan and the Python placeholder scanner with its helpers is deleted."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: characterization
depends_on:
  - "native-cursor-data-path:network:t01"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - "src/py/vertebra/db/adapters/postgres/binds.py:1-53"
  - "src/py/vertebra/db/adapters/postgres/binds.py:124-333"
  - "src/py/vertebra/db/adapters/postgres/binds.py:335-480"
  - "src/py/vertebra/db/adapters/postgres/core.py:203-239"
  - "src/py/vertebra/db/adapters/postgres/pipeline.py:429-445"
  - "src/py/tests/unit/db/test_postgres_options_and_binds.py"
files:
  - "src/py/vertebra/db/adapters/postgres/binds.py"
tests:
  - "src/py/tests/unit/db/test_postgres_options_and_binds.py"
estimated_loc: 150
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
created_at: 2026-11-20T15:30:00Z
commit: null
sources:
  - id: chapter-spec
    resource: .agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md
    title: "Chapter contract for the network engine cursors"
  - id: binds-py
    resource: "src/py/vertebra/db/adapters/postgres/binds.py::compile_postgres_binds"
    title: "Python entry point kept as a thin call"
tags: [postgres, python, cleanup]
---

# Move binds.py onto the Rust bind plan and delete the Python scanner

## Objective & Scope Boundary

Pipelines, Arrow reads, and COPY helpers keep calling `compile_postgres_binds` and get the same result, now computed by `vertebra._postgres.compile_binds`. One implementation of placeholder scanning remains.

Out of scope:

- The query methods of the native drivers. Tasks t07 and t08 move them to the cursor.
- `normalize_postgres_parameter` and `normalize_postgres_parameters`. They stay as they are.
- `core.py` and `__init__.py`. Their imports of the kept names still resolve.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/db/adapters/postgres/binds.py:267-333` is the function body to replace.
- `src/py/vertebra/db/adapters/postgres/binds.py:124-266` and `:335-480` are deleted, except `_copy_json_container` at `:380-386`.
- `src/py/vertebra/db/adapters/postgres/core.py:203-239` and `src/py/vertebra/db/adapters/postgres/pipeline.py:429-445` are the callers that must keep working.

## Contracts & Edge Cases

- `compile_postgres_binds(sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> CompiledPostgresBinds` keeps its signature and return type.
- Body: call the native `compile_binds(sql, parameters)`, pass the returned tuple through `normalize_postgres_parameters`, and build `CompiledPostgresBinds`.
- The native function is resolved once by a module-level accessor decorated with `functools.cache` that imports `vertebra._postgres` through `vertebra.utils.module_loader.ensure_module` with the same arguments as `PostgreSQLCore.resolve_native_module`.
- Delete `_SqlBindPlan`, `_scan_sql_bind_plan`, `_ScanState`, `_is_ident_char`, `_is_dollar_tag_start`, `_is_dollar_tag_char`, `_match_dollar_quote`, `_scan_single_quoted`, `_scan_double_quoted`, `_scan_block_comment`, `_emit_chunk`, `_activate_rewrite`, and any Python-side plan cache in this module.
- Delete tests that call a removed private helper. Keep and extend tests that go through `compile_postgres_binds`.
- Exceptions raised by the native function pass through unchanged. Type and message are the ones t01 copied from this module.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Existing bind tests | Current cases in the test file that call `compile_postgres_binds` | Pass unchanged |
| Named to numeric | `%(a)s, %(b)s, %(a)s` with a mapping | `$1, $2, $1` and a two-item tuple |
| Mutable buffer | `%s` with a `bytearray` | Parameter is `bytes` |
| No parameters | `SELECT 1`, `None` | Same text, empty tuple |
| Removed helper | Import of `_scan_sql_bind_plan` | `ImportError` |

## Verification

- `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_options_and_binds.py -q`
- `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py -q`
- `uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/binds.py`
- `uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/binds.py`
- `test -z "$(git grep -n '_scan_sql_bind_plan' -- src/py)"`

## Acceptance Criteria

- [ ] The public results of `compile_postgres_binds` are unchanged for every existing test.
- [ ] `git grep` finds no reference to any deleted helper under `src/py`.
- [ ] `binds.py` contains no placeholder scanning code.
