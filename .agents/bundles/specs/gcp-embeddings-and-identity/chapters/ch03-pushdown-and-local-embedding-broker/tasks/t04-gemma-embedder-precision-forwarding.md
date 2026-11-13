---
type: Task
id: "gcp-embeddings-and-identity:ch03:t04"
flow_id: gcp-embeddings-and-identity
chapter_id: ch03-pushdown-and-local-embedding-broker
title: "Validate and Forward GemmaEmbedder Precision Through BackendFactory"
description: "Validate GemmaEmbedder precision against {'int8', 'q4', 'bf16', 'fp32'} and forward precision through BackendFactory.options into FastEmbedder.build_backend."
status: stable
state: completed
commit: 5771c8e3
last_verified_commit: 5771c8e3
last_verified_at: "2026-11-13T05:20:15Z"
verification_evidence: "ruff + mypy (12 files) + pytest (404 passed)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "gcp-embeddings-and-identity:ch03:t03"
files:
  - src/py/vertebra/embeddings/_local.py
tests:
  - src/py/tests/unit/embeddings/test_embedding_broker.py
read_set:
  - src/py/vertebra/embeddings/_local.py
  - src/py/vertebra/embeddings/_broker.py
  - src/py/vertebra/embeddings/_models.py
  - src/py/tests/unit/embeddings/test_embedding_broker.py
estimated_loc: 80
tags:
  - embeddings
  - gemma
  - quantization
  - local
sources:
  - id: py-local
    resource: src/py/vertebra/embeddings/_local.py::GemmaEmbedder
    title: "GemmaEmbedder.__init__ and FastEmbedder.build_backend"
---

# Task `t04`: Validate and Forward `GemmaEmbedder` Precision Through `BackendFactory`

## Objective & Scope Boundary

Update `GemmaEmbedder` and `FastEmbedder.build_backend` in `src/py/vertebra/embeddings/_local.py` so configured `precision` (`int8`, `q4`, `bf16`, `fp32`) is strictly validated and forwarded to the ONNX backend constructor rather than discarded per [`../spec.md` §2](../spec.md) [^py-local]. Out of scope: adding new third-party runtime dependencies.

## Bounded Read Manifest

- `src/py/vertebra/embeddings/_local.py` — `GemmaEmbedder.__init__` and `FastEmbedder.build_backend` [^py-local].
- `src/py/vertebra/embeddings/_broker.py` — `BackendFactory` serialization into sidecar JSON pipes.
- `src/py/vertebra/embeddings/_models.py` — `ModelSpec.forbid_fp16` metadata.
- `src/py/tests/unit/embeddings/test_embedding_broker.py` — `GemmaEmbedder` and `FastEmbedder` tests.

## Contracts & Edge Cases

- Preserve the explicit `fp16`/`float16` overflow `ValueError` for Gemma 2 models, and reject any other `norm_prec` not in `{"int8", "q4", "bf16", "fp32"}` (unless `fp16` is explicitly requested on a model with `forbid_fp16=False`) with `ValueError` listing supported precisions.
- Include `"precision": self._precision` in `GemmaEmbedder`'s `BackendFactory.options`.
- Extend `FastEmbedder.build_backend(..., precision: str | None = None)` so `precision in ("int8", "q4")` passes `quantize=True` (and `precision in ("bf16", "fp32")` passes `quantize=False` when accepted by `TextEmbedding.__init__` signature inspection, falling back cleanly if an older stub/version omits `quantize`), ensuring sidecar JSON round-trips preserve `precision`.

## Test Matrix

- Parametrized tests in `src/py/tests/unit/embeddings/test_embedding_broker.py`: `GemmaEmbedder(precision=...)` across `("int8", "q4", "bf16", "fp32")` records `embedder.broker.backend_factory.options["precision"] == precision` and forwards `quantize=True` (`int8`/`q4`) vs `quantize=False` (`bf16`/`fp32`) into a spy `fastembed.TextEmbedding` class; `precision="fp16"` raises activation-overflow `ValueError`; `precision="int4_unknown"` raises `ValueError`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/embeddings/_local.py src/py/tests/unit/embeddings/test_embedding_broker.py && uv run --no-sync mypy src/py/vertebra/embeddings/_local.py && uv run --no-sync pytest src/py/tests/unit/embeddings/test_embedding_broker.py -q
```

## Acceptance Criteria

- [x] `GemmaEmbedder` validates `precision`, serializes it in `BackendFactory.options`, and forwards quantization settings into `FastEmbedder.build_backend` (`R12`).
- [x] Zero regressions in the chapter's fast verification gate.

[^py-local]: `src/py/vertebra/embeddings/_local.py::GemmaEmbedder` — `GemmaEmbedder.__init__` and `FastEmbedder.build_backend`.
