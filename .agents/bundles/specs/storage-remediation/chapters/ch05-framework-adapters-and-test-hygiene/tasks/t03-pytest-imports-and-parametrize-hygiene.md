---
type: Task
id: "storage-remediation:ch05:t03"
chapter_id: ch05-framework-adapters-and-test-hygiene
title: Hoist nested imports and convert subcase for loops and asyncio.run to pytest.mark.parametrize and pytest.mark.anyio
description: Move function-scoped imports to module top-level, replace subcase for loops with @pytest.mark.parametrize, and convert asyncio.run tests to @pytest.mark.anyio across storage, Litestar, and PG DataFusion test modules.
status: stable
state: completed
priority: P2
verification_strategy: static_validation
depends_on:
  - "storage-remediation:ch05:t02"
read_set:
  - src/py/tests/unit/storage/test_storage_store.py
  - src/py/tests/unit/storage/test_signing.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
  - src/py/tests/unit/extensions/test_litestar_plugin.py
  - src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
files: []
tests:
  - src/py/tests/unit/storage/test_storage_store.py
  - src/py/tests/unit/storage/test_signing.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
  - src/py/tests/unit/extensions/test_litestar_plugin.py
  - src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
estimated_loc: 220
plan_revision: 1
plan_commit: fd571e3a
state_revision: 2
claimed_by: executor
claimed_at: "2026-11-09T01:26:10Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T01:34:30Z"
last_verified_commit: cd7f66bf
verification_evidence: "pytest unit/storage + extensions + db (1498 passed, 5 skipped); ruff check/format clean; AST audit: 0 function-scoped imports and 0 asyncio.run across the 5 target files (23 imports, 8 asyncio.run, 6 subcase loops removed; 401 -> 429 collected tests)"
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-09T01:34:30Z"
commit: cd7f66bf
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified:
  - by: executor
    at: "2026-11-09T01:34:30Z"
    commit: cd7f66bf
sources:
  - id: test-storage
    resource: src/py/tests/unit/storage/test_storage_store.py
    title: Object store unit test suite
  - id: test-signing
    resource: src/py/tests/unit/storage/test_signing.py
    title: Presigning unit test suite
  - id: test-gcs-refresh
    resource: src/py/tests/unit/storage/test_gcs_token_refresh.py
    title: GCS token refresh unit test suite
tags:
  - tests
  - pytest
  - hygiene
---

# Task t03: Pytest Top-Level Imports, `@pytest.mark.parametrize`, and `@pytest.mark.anyio` Hygiene

## Objective & Scope Boundary

Remediate test-hygiene findings across `src/py/tests/unit/storage/test_storage_store.py` [^test-storage], `src/py/tests/unit/storage/test_signing.py` [^test-signing], `src/py/tests/unit/storage/test_gcs_token_refresh.py` [^test-gcs-refresh], `src/py/tests/unit/extensions/test_litestar_plugin.py`, and `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py`:
1. Hoist all function-scoped `import ...` / `from ... import ...` statements to module top-level (using `import_optional` at top-level when guarding optional packages).
2. Convert subcase `for` loops inside test functions into `@pytest.mark.parametrize` cases.
3. Replace ad-hoc `asyncio.run(...)` wrappers in async test functions with `@pytest.mark.anyio` (`async def test_...`).

Out of scope: Unrelated test directories outside storage/litestar/datafusion.

## Bounded Read Manifest

- `src/py/tests/unit/storage/test_storage_store.py` [^test-storage].
- `src/py/tests/unit/storage/test_signing.py` [^test-signing].
- `src/py/tests/unit/storage/test_gcs_token_refresh.py` [^test-gcs-refresh].
- `src/py/tests/unit/extensions/test_litestar_plugin.py`.
- `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py`.

## Symbol & Signature Contracts

- Test-only refactoring; zero production files modified (`files: []`).

## Edge Cases & Error Semantics

- Keep zero inline `#` comments across all modified test files.
- Preserve exact assertion strength and negative-path coverage across every parameterized test case.

## Public-Seam Test Blueprint

- **Canonical test files**: `test_storage_store.py`, `test_signing.py`, `test_gcs_token_refresh.py`, `test_litestar_plugin.py`, `test_pg_dataplane_and_datafusion.py`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| AST nested-import & `asyncio.run` audit | Python `ast.walk` over the 5 test files | `0` function-scoped `Import`/`ImportFrom` nodes and `0` `asyncio.run` calls in async-under-test functions. |
| Full parameterized execution | `pytest` across all 5 modules | `exit 0` with expanded parameterized node count. |

## Verification

```bash
uv run --no-sync ruff check src/py/tests/unit/storage src/py/tests/unit/extensions/test_litestar_plugin.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
uv run --no-sync pytest src/py/tests/unit/storage src/py/tests/unit/extensions/test_litestar_plugin.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py -q
```

## Acceptance Criteria

- [x] [cd7f66bf] All 5 test modules have zero function-scoped imports, use `@pytest.mark.parametrize` instead of subcase `for` loops, use `@pytest.mark.anyio` for async tests, contain zero inline `#` comments, and pass `ruff` and `pytest` (R5.3).

## Notes & Discoveries

- [2026-11-09 01:54] `test_vertebra_plugin_overrides_litestar_cli_run_command` exercises `run --help` before `run --workers ...` within one `CliRunner` because Click resolves `run` before Litestar's group callback loads the app in a fresh in-process invocation; a fresh-process `litestar --app ... run --workers 2 --topology shm` resolves Vertebra's `run` override correctly, so the ordering is a `CliRunner` artifact only.
- [2026-11-09 01:34] Remaining `for` loops in the target files are fixture seeding, repetition counters, or helper-double internals rather than assertion subcases, so they stay as loops.
- [2026-11-08 20:55] In `test_litestar_plugin.py`, if a test intentionally verifies sync execution outside an active event loop, keep it synchronous without `asyncio.run`; convert coroutine tests to `@pytest.mark.anyio`.

[^test-storage]: `src/py/tests/unit/storage/test_storage_store.py` — Object store unit test suite.
[^test-signing]: `src/py/tests/unit/storage/test_signing.py` — Presigning unit test suite.
[^test-gcs-refresh]: `src/py/tests/unit/storage/test_gcs_token_refresh.py` — GCS token refresh unit test suite.
