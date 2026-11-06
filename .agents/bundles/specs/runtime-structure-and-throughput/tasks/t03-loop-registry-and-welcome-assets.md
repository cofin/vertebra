---
type: Task
id: runtime-structure-and-throughput:t03
title: Make loop backend selection table-driven and externalize welcome page assets
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
files:
- src/py/vertebra/server/_loops.py
- src/py/vertebra/framework/_app.py
- src/py/vertebra/framework/_welcome.py
- src/py/vertebra/framework/assets/welcome.html
- src/py/vertebra/framework/assets/welcome.css
- pyproject.toml
tests:
- src/py/tests/unit/server/test_runtime_tuning.py
- src/py/tests/unit/framework/test_app.py
plan_revision: 1
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
created_at: '2026-11-06T06:35:00Z'
updated_at: '2026-11-06T06:35:00Z'
commit: null
tags:
- server
- loops
- framework
requirements:
- R3
---

# Make loop backend selection table-driven and externalize welcome page assets

## Objective

Refactor `resolve_loop_backend` and `create_event_loop` onto a table-driven backend spec that scopes free-threaded quarantine checks strictly to candidate loop modules, deduplicate loop-sharded `call_soon_threadsafe` dispatch, evict the debug welcome route strictly on `GET /`, and load welcome HTML/CSS from package data.

## Context

Read `../spec.md` and `.agents/bundles/knowledge/patterns/python/hot-path-and-async-dispatch.md`. Ensure `pyproject.toml` wheel/sdist includes `src/py/vertebra/framework/assets/*.{html,css}` and optional Rust extension `pyproject.toml` manifests continue excluding Python package files cleanly.

## Steps

1. Write failing tests in `test_runtime_tuning.py` (verifying `resolve_loop_backend("auto")` selects `rloop` even when an unrelated module prefix is quarantined, and skips `rloop`/`uvloop` only when that loop module prefix is quarantined) and `test_app.py` (verifying `POST /` or `GET /health` keeps the debug `GET /` welcome handler active until `GET /` or `ANY /` is registered).
2. Replace `resolve_loop_backend` / `create_event_loop` branching in `src/py/vertebra/server/_loops.py` with a frozen `LoopBackendSpec` table and consolidate repeated bucket + `call_soon_threadsafe` blocks in `AsyncLoopShardedResource` and `SyncLoopShardedResource`.
3. Move inline HTML/CSS from `src/py/vertebra/framework/_welcome.py` to `src/py/vertebra/framework/assets/{welcome.html,welcome.css}` loaded via `importlib.resources`, register package data in `pyproject.toml`, and refine `App.add_route` welcome eviction in `src/py/vertebra/framework/_app.py`.
4. Run verification and commit `refactor(server,framework): table-drive loop selection and move welcome assets to package data`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/server/test_runtime_tuning.py src/py/tests/unit/framework/test_app.py -q
uv run --no-sync ruff check src/py/vertebra/server/_loops.py src/py/vertebra/framework/_app.py src/py/vertebra/framework/_welcome.py
uv run --no-sync mypy src/py/vertebra/server/_loops.py src/py/vertebra/framework/_app.py src/py/vertebra/framework/_welcome.py
```

## Acceptance Criteria

- [ ] R3: Unrelated quarantined C extensions no longer disable `rloop` auto-selection; welcome HTML/CSS loads from package data and evicts strictly when `GET /` is claimed.

## Notes & Discoveries

None.
