---
type: Task
id: native-postgres-driver:t26
title: Verify raw-session wheel parity and isolated free-threaded imports
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- native-postgres-driver:t25
files:
- src/py/tests/unit/test_freethreaded_gil.py
- .github/workflows/release.yaml
tests:
- src/py/tests/unit/test_freethreaded_gil.py::test_postgres_import_preserves_disabled_gil_in_child
plan_revision: 2
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-05T18:25:07Z'
updated_at: '2026-11-06T06:30:00Z'
commit: null
tags:
- postgres
- connection
- native
requirements:
- R1
- R6
- R7
- R26
---

# Verify raw-session wheel parity and isolated free-threaded imports

## Objective

Verify raw-session wheel parity and isolated free-threaded imports, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 260 lines; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.



## Steps

1. Add test_postgres_import_preserves_disabled_gil_in_child in existing test_freethreaded_gil.py. Gate only by build capability sysconfig.get_config_var("Py_GIL_DISABLED"), never parent current GIL state. On3.14t/3.15t run [sys.executable,"-I","-c",script] with check=True,capture_output=True,text=True,timeout=30, child environment without PYTHON_GIL and no -X gil override. Child asserts sys._is_gil_enabled() isFalse before first import vertebra._postgres and again afterward. Missing checker/import fails. FREE_THREADED/GIL_USED metadata alone proves nothing. Ordinary GIL builds may skip this one test; required FT lanes execute it. Existing test-python matrix runs it through make py-test.
2. Replace postgres release exports FREE_THREADED with SyncPostgresClient AsyncPostgresClient and update combined namespace/version dictionary. Installed smoke checks pure constructors, initial closed/parameters and venv module paths, without remote connections. Later full-cutover tasks add final pool/query exports; this is not complete-driver acceptance or permission to publish.
3. Run exact fresh installed-wheel recipe below. Bun must already be installed, then make ts-install precedes make ts-build. Build core/postgres wheels, install explicit local paths into fresh venv, verify optional extra resolution offline, install locked test/lint dependencies and run isolated import/stubtest outside checkout. Final core-only uninstall and full query/PG14–18 acceptance stay in later acceptance worksheets.
4. Rebuild against each matching3.14t/3.15t environment before isolated-child and cancellation tests, then restore3.12 as shown. Commit test(postgres): verify wheel stubs and free-threaded import isolation.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
make dev
uv run --no-sync python -m mypy.stubtest vertebra._postgres
uv sync --python 3.14t --all-extras --dev --group doc
UV_PYTHON=3.14t UV_NO_SYNC=1 make dev
UV_PYTHON=3.14t uv run --no-sync python -m pytest src/py/tests/unit/test_freethreaded_gil.py -k postgres_import_preserves_disabled_gil_in_child -q
UV_PYTHON=3.14t uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_driver.py -k native_async_cancel -q
uv sync --python 3.15t --all-extras --dev --group doc
UV_PYTHON=3.15t UV_NO_SYNC=1 make dev
UV_PYTHON=3.15t uv run --no-sync python -m pytest src/py/tests/unit/test_freethreaded_gil.py -k postgres_import_preserves_disabled_gil_in_child -q
UV_PYTHON=3.15t uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_driver.py -k native_async_cancel -q
uv sync --python 3.12 --all-extras --dev --group doc
UV_PYTHON=3.12 UV_NO_SYNC=1 make dev
```

Fresh installed-wheel lane from repository root, with Bun installed and PYTHONPATH unset for isolated checks:

```bash
PG_WHEEL_DIR="$(mktemp -d /tmp/vertebra-postgres-wheel-XXXXXX)"
make ts-install
make ts-build
uv build --wheel --out-dir "$PG_WHEEL_DIR/dist"
uv build src/rs/vertebra-postgres --wheel --out-dir "$PG_WHEEL_DIR/dist"
uv export --only-group test --only-group lint --no-emit-project --format requirements-txt --output-file "$PG_WHEEL_DIR/test-requirements.txt"
uv venv --python 3.12 "$PG_WHEEL_DIR/venv"
uv pip install --python "$PG_WHEEL_DIR/venv/bin/python" "$PG_WHEEL_DIR"/dist/vertebra_core-*.whl "$PG_WHEEL_DIR"/dist/vertebra_postgres-*.whl
uv pip install --python "$PG_WHEEL_DIR/venv/bin/python" --no-index --find-links "$PG_WHEEL_DIR/dist" 'vertebra-core[postgres]'
uv pip install --python "$PG_WHEEL_DIR/venv/bin/python" -r "$PG_WHEEL_DIR/test-requirements.txt"
cd "$PG_WHEEL_DIR"
"$PG_WHEEL_DIR/venv/bin/python" -I -c 'import pathlib,sys,importlib.metadata,vertebra,vertebra._postgres as p; assert pathlib.Path(p.__file__).is_relative_to(sys.prefix); assert pathlib.Path(vertebra.__file__).is_relative_to(sys.prefix); assert importlib.metadata.version("vertebra-postgres")==importlib.metadata.version("vertebra-core"); assert p.SyncPostgresClient(user="u",database="d").closed; assert p.AsyncPostgresClient(user="u",database="d").parameters=={}'
"$PG_WHEEL_DIR/venv/bin/python" -I -m mypy.stubtest vertebra._postgres
```

Final exit0 is required. These commands are planned, not executed evidence. Record each command/exit and public assertions during execution. Source rebuild precedes Python tests; never validate a stale extension. No external write, workflow dispatch, tag mutation or push is authorized here.

## Acceptance Criteria

- [ ] R1, R6, R7, R26: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] The concrete Steps contracts and negative cases pass through the named canonical tests.
- [ ] No duplicated protocol/crypto/runtime engine or fake placeholder client is introduced.
- [ ] Owned changes fit one invocation/commit; direct Markdown task state and parent checklist are synchronized after verification.

## Notes & Discoveries

None.
