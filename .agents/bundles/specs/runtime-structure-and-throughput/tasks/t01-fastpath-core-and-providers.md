---
type: Task
id: runtime-structure-and-throughput:t01
title: Extract shared fastpath core and FastPathProvider registry
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
files:
- src/py/vertebra/server/_fastpath_core.py
- src/py/vertebra/server/_fastpath.py
- src/py/vertebra/extensions/litestar/fastpath.py
- src/py/vertebra/extensions/starlette/fastpath.py
- src/py/vertebra/extensions/fastapi/fastpath.py
- src/py/vertebra/extensions/fastapi/dependencies.py
- src/py/vertebra/extensions/flask/fastpath.py
- src/py/vertebra/extensions/django/fastpath.py
- src/py/vertebra/utils/type_guards.py
tests:
- src/py/tests/unit/server/test_litestar_direct_fastpath.py
- src/py/tests/unit/extensions/test_starlette_plugin.py
- src/py/tests/unit/extensions/test_fastapi_plugin.py
- src/py/tests/unit/extensions/test_flask_plugin.py
- src/py/tests/unit/extensions/test_django_plugin.py
- src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
- src/py/tests/unit/utils/test_dispatch_schema_converters.py
plan_revision: 1
plan_commit: "af319da17dec0148a18532952cb7ea2a91d1c9d7"
state_revision: 2
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-06T15:22:00Z'
last_verified_commit: "4a5512d426a536fda14168e6752c0533335eaae3"
verification_evidence: 260 passed across fastpath, framework plugin, CLI, and type_guards unit suites; ruff + mypy + review_setup clean
created_at: '2026-11-06T06:35:00Z'
updated_at: '2026-11-06T15:22:00Z'
commit: "4a5512d426a536fda14168e6752c0533335eaae3"
tags:
- fastpath
- server
- extensions
requirements:
- R1
---

# Extract shared fastpath core and FastPathProvider registry

## Objective

Create `src/py/vertebra/server/_fastpath_core.py` with shared route compilation, scalar parameter coercion, `step_coro_sync`, and `FastPathProvider` registration so all five framework fastpaths delegate to one core and attach a single `__vertebra_fastpath__` attribute.

## Context

Read `../spec.md`, `.agents/bundles/knowledge/patterns/python/hot-path-and-async-dispatch.md`, and `.agents/skills/python-framework/SKILL.md`. Do not import optional web frameworks eagerly at server startup; resolve registered or lazy `FastPathProvider` entries only when candidate modules are present in `sys.modules` or explicitly invoked.

## Steps

1. Write failing tests in `test_litestar_direct_fastpath.py` and the framework plugin suites asserting `FastPathProvider` protocol registration, `FastPath.from_app` explicit construction, single canonical `__vertebra_fastpath__` attachment (with `_vertebra_fastpath` removed), and shared `vertebra.utils.type_guards` predicates.
2. Create `src/py/vertebra/server/_fastpath_core.py` containing `FastPathProvider`, `DirectPlan`, shared scalar path/query converters, `step_coro_sync`, exception-serializer hooks, and the provider registry.
3. Consolidate duplicated coroutine/generator/struct type guards into `src/py/vertebra/utils/type_guards.py` and refactor `extensions/{litestar,starlette,fastapi,flask,django}/fastpath.py` plus `server/_fastpath.py::attach_framework_fastpath` to use `_fastpath_core` and `__vertebra_fastpath__`.
4. Run verification and commit `refactor(server): unify framework fastpaths behind FastPathProvider core`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/server/test_litestar_direct_fastpath.py src/py/tests/unit/extensions/test_starlette_plugin.py src/py/tests/unit/extensions/test_fastapi_plugin.py src/py/tests/unit/extensions/test_flask_plugin.py src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/utils/test_dispatch_schema_converters.py -q
uv run --no-sync ruff check src/py/vertebra/server src/py/vertebra/extensions src/py/vertebra/utils/type_guards.py
uv run --no-sync mypy src/py/vertebra/server src/py/vertebra/extensions src/py/vertebra/utils/type_guards.py
```

## Acceptance Criteria

- [x] R1: `FastPathProvider` registry replaces class-name string sniffing in `attach_framework_fastpath` and attaches only `__vertebra_fastpath__`.
- [x] Shared scalar coercion, `DirectPlan`, and `step_coro_sync` live in `src/py/vertebra/server/_fastpath_core.py` without framework regressions.
- [x] Zero inline `#` comments or function-scoped imports are introduced.

## Notes & Discoveries

- Promoted `get_starlette_request_cls` to public API in `vertebra.extensions.starlette.fastpath` and routed `vertebra.extensions.fastapi.dependencies` through `_fastpath_core.scalar_query_converter` and `vertebra.utils.type_guards` so zero cross-module private `_...` imports remain across fastpath modules.
