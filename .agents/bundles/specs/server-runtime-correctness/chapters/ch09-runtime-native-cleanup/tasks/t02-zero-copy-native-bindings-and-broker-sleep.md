---
type: Task
id: server-runtime-correctness:ch09:t02
title: Bind Native Zero-Copy Helpers Directly and Remove the Broker Per-Chunk Sleep
description: Import the five zero-copy helpers directly from vertebra._vertebra with the _py_* fallbacks deleted, and delete the fixed asyncio.sleep(0.002) after each chunk in EmbeddingBroker.submit_batch.
status: draft
state: pending
priority: P2
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch09-runtime-native-cleanup/spec.md
  - src/py/vertebra/utils/serializers/_zero_copy.py
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/embeddings/_broker.py
  - src/py/tests/unit/db/test_vector_wire_codecs.py
  - src/py/tests/unit/embeddings/test_embedding_broker.py
files:
  - src/py/vertebra/utils/serializers/_zero_copy.py
  - src/py/vertebra/embeddings/_broker.py
tests:
  - src/py/tests/unit/db/test_vector_wire_codecs.py
  - src/py/tests/unit/embeddings/test_embedding_broker.py
estimated_loc: 90
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T01:35:00Z"
updated_at: "2026-11-12T01:35:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T01:35:00Z"
sources:
  - id: zero-copy-py
    resource: src/py/vertebra/utils/serializers/_zero_copy.py
    title: Zero-copy codec module with _py_* fallbacks
  - id: broker-py
    resource: src/py/vertebra/embeddings/_broker.py::EmbeddingBroker.submit_batch
    title: Bulk submission with a fixed 2 ms sleep per chunk
tags:
  - serializers
  - embeddings
  - cleanup
---

# Task t02: Bind Native Zero-Copy Helpers Directly and Remove the Broker Per-Chunk Sleep

## Objective & Scope Boundary

Two independent single-file deletions on hot paths: `_zero_copy.py` imports its five native helpers directly and deletes the `_py_*` fallbacks [^zero-copy-py]; `EmbeddingBroker.submit_batch` deletes `await asyncio.sleep(0.002)` [^broker-py]. Out of scope: `import_optional` internals (`t03`), the broker subprocess path, and preemption order.

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `src/py/vertebra/utils/serializers/_zero_copy.py` — `_native_vtb = import_optional("vertebra._vertebra")`, `_py_pack_grpc_lpm_frame`, `_py_instantiate_struct_vectorcall`, `_py_hydrate_one_vectorcall`, `_py_hydrate_rows_vectorcall`, `_py_orm_to_struct_vectorcall`, and the `getattr(...) or _py_*` bindings.
- `src/py/vertebra/_vertebra.pyi` — declarations of the five helpers.
- `src/py/vertebra/embeddings/_broker.py::EmbeddingBroker.submit_batch` — per-chunk loop.
- `src/py/tests/unit/db/test_vector_wire_codecs.py`, `src/py/tests/unit/embeddings/test_embedding_broker.py` — canonical tests.

## Symbol & Signature Contracts

- `_zero_copy.py` top-level: `from vertebra._vertebra import hydrate_one_vectorcall, hydrate_rows_vectorcall, instantiate_struct_vectorcall, orm_to_struct_vectorcall, pack_grpc_lpm_frame`; delete `_native_vtb`, the five `_py_*` functions, and the `import_optional` import when unused. Module-level names and `__all__` are unchanged.
- `submit_batch` loop keeps: `_drain_pending_once` when pending, `await asyncio.sleep(0)`, `_drain_pending_once` when pending, `asyncio.to_thread(self._execute_length_sorted_rows, chunk_texts)`, row placement, `_drain_pending_once` when pending. Only the `asyncio.sleep(0.002)` line is removed.

## Edge Cases & Error Semantics

- `vertebra._vertebra` missing raises `ImportError` at import of `_zero_copy`.
- An interactive request queued during a chunk still drains before the next chunk.
- An empty `texts` sequence still raises `ValueError("texts must not be empty")`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/db/test_vector_wire_codecs.py`, `src/py/tests/unit/embeddings/test_embedding_broker.py`; dependency category `unit`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Native binding identity, parametrized over `pack_grpc_lpm_frame`, `hydrate_one_vectorcall`, `hydrate_rows_vectorcall`, `instantiate_struct_vectorcall`, `orm_to_struct_vectorcall` | `getattr(_zero_copy, name)` | `is getattr(vertebra._vertebra, name)` |
| No fallbacks | `[n for n in dir(_zero_copy) if n.startswith("_py_")]` | `[]` |
| LPM frame | `pack_grpc_lpm_frame(b"abc")` | `b"\x00\x00\x00\x00\x03abc"` |
| No fixed sleep | `submit_batch` over 3 chunks (`bulk_chunk_size=1`, 3 texts) with `asyncio.sleep` patched to record arguments | Recorded delays are `[0, 0, 0]` |
| Preemption kept | Interactive `submit` queued while chunk 1 runs | Interactive future resolves before chunk 2 starts |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_vector_wire_codecs.py src/py/tests/unit/embeddings/test_embedding_broker.py -q`
- Final Green + lint:
  ```bash
  uv run --no-sync pytest src/py/tests/unit/db/test_vector_wire_codecs.py src/py/tests/unit/embeddings -q
  uv run --no-sync ruff check src/py/vertebra/utils/serializers src/py/vertebra/embeddings
  uv run --no-sync mypy src/py/vertebra/utils/serializers src/py/vertebra/embeddings
  uv run --no-sync python -m mypy.stubtest vertebra._vertebra
  ```
- Planned local commit: `refactor: bind native zero-copy helpers directly and drop broker chunk sleep`

## Acceptance Criteria

- [ ] `_zero_copy.py` contains no `_py_*` functions and binds the native helpers directly (R21).
- [ ] `EmbeddingBroker.submit_batch` has no fixed sleep (R23).

## Notes & Discoveries

[^zero-copy-py]: `src/py/vertebra/utils/serializers/_zero_copy.py` — Zero-copy codec module with `_py_*` fallbacks.
[^broker-py]: `src/py/vertebra/embeddings/_broker.py::EmbeddingBroker.submit_batch` — Bulk submission with a fixed 2 ms sleep per chunk.
