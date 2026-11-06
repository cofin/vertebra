---
type: Task
id: runtime-structure-and-throughput:t07
title: Add Rust HTTP/2 VertexEmbedder transport, bounded EmbeddingBroker offload, and batch Oracle pushdown
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
- runtime-structure-and-throughput:t04
files:
- src/rs/vertebra-core/src/gcp_auth.rs
- src/rs/vertebra-py/Cargo.toml
- src/rs/vertebra-py/src/gcp_auth.rs
- src/rs/vertebra-py/src/lib.rs
- src/py/vertebra/_vertebra.pyi
- src/py/vertebra/embeddings/_base.py
- src/py/vertebra/embeddings/_google.py
- src/py/vertebra/embeddings/_broker.py
- src/py/vertebra/db/adapters/oracle/embeddings.py
tests:
- src/py/tests/unit/embeddings/test_google_embedders.py
- src/py/tests/unit/embeddings/test_embedding_broker.py
- src/py/tests/unit/db/test_enterprise_olap_drivers.py
plan_revision: 1
plan_commit: "af319da17dec0148a18532952cb7ea2a91d1c9d7"
state_revision: 2
claimed_by: agent:executor
claimed_at: '2026-11-06T18:45:00Z'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-06T18:57:36Z'
last_verified_commit: "e99a7128a0625f36b47036ee520c8a46cbded84e"
verification_evidence:
  command: cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py --features gcp && make dev && uv run --no-sync python -m mypy.stubtest vertebra._vertebra && uv run --no-sync pytest src/py/tests/unit/embeddings/test_google_embedders.py src/py/tests/unit/embeddings/test_embedding_broker.py src/py/tests/unit/db/test_enterprise_olap_drivers.py -q
  exit_code: 0
  summary: 11 Rust unit tests and 177 Python unit tests passed; stubtest, ruff, rustfmt, and mypy clean.
created_at: '2026-11-06T06:35:00Z'
updated_at: '2026-11-06T18:57:36Z'
commit: "e99a7128a0625f36b47036ee520c8a46cbded84e"
tags:
- embeddings
- http2
- oracle
requirements:
- R7
---

# Add Rust HTTP/2 VertexEmbedder transport, bounded EmbeddingBroker offload, and batch Oracle pushdown

## Objective

Move `VertexEmbedder`'s default transport onto a Rust HTTP/2 gRPC-framed client in `vertebra._vertebra`, reuse a persistent background portal in `Embedder._run_sync` without double `__init__`, offload in-process `EmbeddingBroker` inference via `asyncio.to_thread` with a bounded queue, forward inline media bytes in `GeminiEmbedder.embed_multimodal`, and batch `OracleEmbeddingPushdown` calls via `DBMS_VECTOR.UTL_TO_EMBEDDINGS`.

## Context

Read `../spec.md` and `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md`. Keep `VertexEmbedder`'s injected `transport: GrpcTransportCallable | None` seam working for unit tests while replacing stdlib `urllib.request` with the native Rust HTTP/2 client by default.

## Steps

1. Write failing unit tests in `test_google_embedders.py` (`Embedder()` single initialization, persistent `_run_sync` portal reuse, `GeminiEmbedder.embed_multimodal` inline `MultimodalPart.data` base64/protobuf encoding, and Rust HTTP/2 transport trailer status handling), `test_embedding_broker.py` (bounded queue backpressure and non-blocking `asyncio.to_thread` local inference when `isolate_gil_subprocess=False`), and `test_enterprise_olap_drivers.py` (`OracleEmbeddingPushdown.embed_batch` issuing a single batched `DBMS_VECTOR.UTL_TO_EMBEDDINGS` SQL statement instead of `N` round trips).
2. Expose a connection-pooled Rust HTTP/2 unary gRPC/JSON post helper in `src/rs/vertebra-py/src/gcp_auth.rs` (and `_vertebra.pyi`) using `AsyncCompletionQueue` / `py.detach`, and wire `VertexEmbedder._default_http_transport` + `GeminiEmbedder.embed_multimodal` in `src/py/vertebra/embeddings/_google.py`.
3. Update `src/py/vertebra/embeddings/_base.py` (`Embedder.__new__` and persistent `BlockingPortal` in `_run_sync`), `src/py/vertebra/embeddings/_broker.py` (`max_pending_requests` bound + `asyncio.to_thread` local inference), and `src/py/vertebra/db/adapters/oracle/embeddings.py` (`DBMS_VECTOR.UTL_TO_EMBEDDINGS` batch SQL).
4. Run verification and commit `perf(embeddings): add Rust HTTP/2 Vertex transport, bounded broker offload, and batch Oracle pushdown`.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py --features gcp
make dev
uv run --no-sync python -m mypy.stubtest vertebra._vertebra
uv run --no-sync pytest src/py/tests/unit/embeddings/test_google_embedders.py src/py/tests/unit/embeddings/test_embedding_broker.py src/py/tests/unit/db/test_enterprise_olap_drivers.py -q
```

## Acceptance Criteria

- [x] R7: `VertexEmbedder` uses Rust HTTP/2 by default, `GeminiEmbedder.embed_multimodal` transmits inline media bytes, `Embedder._run_sync` reuses a persistent portal, `EmbeddingBroker` offloads in-process inference with bounded queue depth, and `OracleEmbeddingPushdown` batches via `DBMS_VECTOR.UTL_TO_EMBEDDINGS`.

## Notes & Discoveries

- Exposed `gcp_http2_post_sync` and `gcp_http2_post_async` on `vertebra._vertebra` (backed by connection-pooled `reqwest::Client` instances with loopback proxy bypass for local test servers) and wired `VertexEmbedder._default_http_transport` to `gcp_http2_post_async`.
- Simplified `Embedder.__new__` to return `object.__new__(vertex_cls)` directly without pre-calling `VertexEmbedder.__init__`, and added a persistent background event-loop thread (`_ensure_sync_portal_loop`) in `Embedder._run_sync`.
- Forwarded full base64 inline media payloads in `GeminiEmbedder.embed_multimodal`, added `max_pending_requests` backpressure and `asyncio.to_thread` local inference offload in `EmbeddingBroker`, and batched `OracleEmbeddingPushdown.embed_documents` / `embed_batch` via `DBMS_VECTOR.UTL_TO_EMBEDDINGS`.
