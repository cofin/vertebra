---
type: Task
flow_id: native-mysql-driver
task_id: t05
title: "Wire vertebra-mysql into Makefile, wheel isolation tests, and free-threaded GIL gates"
description: "Add mysql-test and mysql-wheel-check targets in Makefile, register vertebra._mysql in test_freethreaded_gil.py, and verify optional driver wheel isolation in test_optional_driver_wheels.py."
status: stable
state: completed
verification_strategy: tdd
Verification-Exempt-Reason: null
dependencies:
  - t04
files_to_modify:
  - Makefile
  - src/py/tests/unit/test_freethreaded_gil.py
  - src/py/tests/unit/db/test_optional_driver_wheels.py
read_set:
  - .agents/bundles/specs/native-mysql-driver/spec.md
  - Makefile
  - src/py/tests/unit/test_freethreaded_gil.py
  - src/py/tests/unit/build/test_packaging_and_pgo.py
commands_to_run:
  - "uv run pytest src/py/tests/unit/test_freethreaded_gil.py src/py/tests/unit/db/test_optional_driver_wheels.py src/py/tests/unit/build/test_packaging_and_pgo.py -q"
  - "uv run ruff check src/py/tests/unit/test_freethreaded_gil.py src/py/tests/unit/db/test_optional_driver_wheels.py"
commit_sha: 18138e92
created_at: "2026-11-09T16:50:00Z"
updated_at: "2026-11-09T16:50:00Z"
sources:
  - id: mysql-spec
    resource: .agents/bundles/specs/native-mysql-driver/spec.md
    title: Native MySQL driver specification
  - id: makefile
    resource: Makefile
    title: Repository polyglot build and verification targets
  - id: gil-test
    resource: src/py/tests/unit/test_freethreaded_gil.py
    title: Free-threaded GIL and Py_mod_gil_not_used verification suite
---

# Task t05: Wire `vertebra-mysql` into `Makefile`, wheel isolation tests, and free-threaded GIL gates

<!-- truth: start -->
- `Makefile` provides `mysql-test` and `mysql-wheel-check` targets and wires `vertebra-mysql` into `dev`, `build`, `py-test`, `lint`, and `lint-all` [^mysql-spec] [^makefile].
- `src/py/tests/unit/test_freethreaded_gil.py` and `src/py/tests/unit/db/test_optional_driver_wheels.py` verify `vertebra._mysql` declares `gil_used = false`, exports `FREE_THREADED` and `GIL_USED`, and isolates `_mysql.pyi` and `.so` artifacts across sibling wheels [^gil-test].
<!-- truth: end -->

## Context

Task `t05` wires `vertebra-mysql` into the repository's build/verification targets (`Makefile`), free-threaded Python 3.14t/3.15t GIL audit (`test_freethreaded_gil.py`), and optional wheel isolation test suite (`test_optional_driver_wheels.py`) [^mysql-spec] [^makefile] [^gil-test].

## Public Seam & TDD Plan

1. **Red**:
   - Add `vertebra._mysql` to `NATIVE_MODULES` in `src/py/tests/unit/test_freethreaded_gil.py` and author `src/py/tests/unit/db/test_optional_driver_wheels.py` checking `vertebra-mysql` wheel metadata, sibling `.pyi` exclusions, and `MissingDependencyError` hints.
2. **Green**:
   - Add `mysql-test` and `mysql-wheel-check` to `Makefile` and ensure `vertebra-mysql` passes all wheel isolation and free-threaded GIL checks.
3. **Refactor & Verify**:
   - Run `pytest src/py/tests/unit/test_freethreaded_gil.py src/py/tests/unit/db/test_optional_driver_wheels.py src/py/tests/unit/build/test_packaging_and_pgo.py -q`.

## Acceptance Criteria

- [x] `Makefile` defines `mysql-test` and `mysql-wheel-check`.
- [x] `pytest src/py/tests/unit/test_freethreaded_gil.py src/py/tests/unit/db/test_optional_driver_wheels.py src/py/tests/unit/build/test_packaging_and_pgo.py` passes.

[^mysql-spec]: `.agents/bundles/specs/native-mysql-driver/spec.md` — Native MySQL driver specification.
[^makefile]: `Makefile` — Repository polyglot build and verification targets.
[^gil-test]: `src/py/tests/unit/test_freethreaded_gil.py` — Free-threaded GIL and Py_mod_gil_not_used verification suite.
