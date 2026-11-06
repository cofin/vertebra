---
type: Task
id: runtime-structure-and-throughput:t02
title: Enforce hot-path body size cap, zero-copy Arrow responses, and raw byte headers
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
- runtime-structure-and-throughput:t01
files:
- src/py/vertebra/server/_fastpath.py
- src/py/vertebra/server/_dispatch.py
- src/py/vertebra/server/_asgi/_queues.py
- src/py/vertebra/server/_asgi/_wire.py
- src/py/vertebra/server/_asgi/_runner.py
- src/py/vertebra/extensions/litestar/fastpath.py
- src/py/vertebra/extensions/fastapi/fastpath.py
- src/py/vertebra/extensions/starlette/fastpath.py
- src/py/vertebra/extensions/flask/fastpath.py
- src/py/vertebra/extensions/django/fastpath.py
tests:
- src/py/tests/unit/server/test_stage1_hotpath_and_314t.py
- src/py/tests/unit/server/test_zero_copy_buffer.py
- src/py/tests/unit/server/test_asgi.py
plan_revision: 1
plan_commit: "af319da17dec0148a18532952cb7ea2a91d1c9d7"
state_revision: 2
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-06T15:42:25Z'
last_verified_commit: "23f0b41da86d2ad3f113255a50dcbe0102476c73"
verification_evidence: "62 passed in test_stage1_hotpath_and_314t.py, test_zero_copy_buffer.py, and test_asgi.py; ruff and mypy exited 0 across 172 source files."
created_at: '2026-11-06T06:35:00Z'
updated_at: '2026-11-06T15:42:25Z'
commit: "23f0b41da86d2ad3f113255a50dcbe0102476c73"
tags:
- fastpath
- asgi
- zero-copy
requirements:
- R2
---

# Enforce hot-path body size cap, zero-copy Arrow responses, and raw byte headers

## Objective

Bound streaming ASGI body reads in `extract_scope_body_fast`, read prebuffered bodies once in `wrap_asgi_fastpath`, return zero-copy `ArrowCursor` buffer views in `serialize_fastpath_special_result`, and preserve `bytes` header pairs in `AsgiSend.send_raw_response`.

## Context

Read `../spec.md`, `t01-fastpath-core-and-providers.md`, and `.agents/bundles/knowledge/patterns/python/hot-path-and-async-dispatch.md`. `vertebra._vertebra.ArrowCursor` exposes `buffer_view()` implementing the PEP 3118 buffer protocol directly accepted by the Rust ASGI response writer.

## Steps

1. Add failing tests in `test_stage1_hotpath_and_314t.py`, `test_zero_copy_buffer.py`, and `test_asgi.py` covering chunked `receive()` payloads exceeding `max_body_size` (`413`), single invocation of `scope["_vertebra_body_bytes"]`, zero-copy `ArrowCursor` buffer return (`memoryview`/buffer object without `bytes(...)` copy), and raw `tuple[bytes, bytes]` header preservation in `AsgiSend.send_raw_response`.
2. Update `extract_scope_body_fast` to enforce `max_body_size` (raising a typed `PayloadTooLarge` fastpath exception mapped to HTTP `413`) and pass prebuffered body bytes from `wrap_asgi_fastpath` into `extract_scope_body_fast` without calling `body_bytes_fn()` twice.
3. Dispatch `serialize_fastpath_special_result` via `isinstance(result, ArrowCursor)` (and exact type lookup) returning `result.buffer_view()` directly, and preserve `bytes` header pairs in `src/py/vertebra/server/_asgi/_queues.py::AsgiSend.send_raw_response`.
4. Run verification and commit `perf(server): cap fastpath body buffering and preserve zero-copy Arrow and byte headers`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/server/test_stage1_hotpath_and_314t.py src/py/tests/unit/server/test_zero_copy_buffer.py src/py/tests/unit/server/test_asgi.py -q
uv run --no-sync ruff check src/py/vertebra/server
uv run --no-sync mypy src/py/vertebra/server
```

## Acceptance Criteria

- [x] R2: Oversized streaming bodies fail closed with HTTP `413` before unbounded concatenation.
- [x] `body_bytes_fn()` executes at most once per fastpath request, `ArrowCursor` responses avoid `bytes(...)` copies, and `bytes` headers are not decoded/re-encoded.

## Notes & Discoveries

None.
