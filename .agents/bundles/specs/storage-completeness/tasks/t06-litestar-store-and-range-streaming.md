---
type: Task
id: storage-completeness:t06
title: Add Litestar AsyncObjectStore Store, static-files adapter, and non-blocking Range/416 streaming
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- storage-completeness:t02
files:
- src/py/vertebra/storage/_streaming.py
- src/py/vertebra/storage/__init__.py
- src/py/vertebra/extensions/litestar/store.py
- src/py/vertebra/extensions/litestar/__init__.py
tests:
- src/py/tests/unit/extensions/test_litestar_plugin.py
- src/py/tests/unit/server/test_zero_copy_buffer.py
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
created_at: '2026-11-06T06:38:00Z'
updated_at: '2026-11-06T06:38:00Z'
commit: null
tags:
- storage
- litestar
- streaming
requirements:
- R7
---

# Add Litestar AsyncObjectStore Store, static-files adapter, and non-blocking Range/416 streaming

## Objective

Implement `ObjectStoreLitestarStore` (`litestar.stores.base.Store` backed by `AsyncObjectStore` with TTL metadata), an `ObjectStoreFileSystemAdapter` for Litestar static files, and non-blocking `StreamingStorageResponse` / `to_fastpath_tuple` supporting HTTP `Range` (`206 Partial Content`, `Content-Range`, `Accept-Ranges: bytes`) and `416 Range Not Satisfiable`.

## Context

Read `../spec.md`, `t02-unified-gcp-auth-signing-and-fail-closed.md`, and `.agents/skills/litestar/SKILL.md`. All async reads in `StreamingStorageResponse` and `ObjectStoreLitestarStore` must use `AsyncObjectStore` without blocking the event loop.

## Steps

1. Write failing tests in `test_litestar_plugin.py` and `test_zero_copy_buffer.py` covering `ObjectStoreLitestarStore` (`set`, `get`, `delete`, `delete_all`, `exists`, `expires_in` TTL expiration), Litestar static file serving through `ObjectStoreFileSystemAdapter`, and `StreamingStorageResponse` returning `200`, `206` (`bytes=0-15`, `bytes=-8`, `bytes=8-`), and `416` (`Content-Range: bytes */<size>`) on out-of-bounds or inverted `Range` headers.
2. Implement `StreamingStorageResponse` and `Range` header parsing/streaming in `src/py/vertebra/storage/_streaming.py` (re-exported in `src/py/vertebra/storage/__init__.py`).
3. Implement `ObjectStoreLitestarStore` and `ObjectStoreFileSystemAdapter` in `src/py/vertebra/extensions/litestar/store.py` and export them from `vertebra.extensions.litestar`.
4. Run verification and commit `feat(litestar,storage): add ObjectStore Litestar Store, static-files adapter, and Range/416 streaming`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_plugin.py src/py/tests/unit/server/test_zero_copy_buffer.py -q
uv run --no-sync ruff check src/py/vertebra/storage/_streaming.py src/py/vertebra/extensions/litestar/store.py
uv run --no-sync mypy src/py/vertebra/storage/_streaming.py src/py/vertebra/extensions/litestar/store.py
```

## Acceptance Criteria

- [ ] R7: `ObjectStoreLitestarStore`, `ObjectStoreFileSystemAdapter`, and non-blocking `StreamingStorageResponse` (`200`/`206`/`416`) pass full Litestar and zero-copy streaming suites.

## Notes & Discoveries

None.
