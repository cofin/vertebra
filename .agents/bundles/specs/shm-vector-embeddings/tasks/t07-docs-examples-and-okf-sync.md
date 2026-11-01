---
type: Task
task_id: t07-docs-examples-and-okf-sync
flow_id: shm-vector-embeddings
title: "Embeddings Documentation, Runnable Example & OKF v0.2 Knowledge Base Sync"
description: "Author docs/usage/embeddings-and-vectors.md, docs/examples/embeddings_app.py, update zensical.toml, and synchronize OKF v0.2 knowledge chapters for shared-memory vectors, embeddings, active ADC credential refresh, and vtb ai CLI."
scope: task
status: draft
state: pending
strategy: tdd
depends_on:
  - t04-google-grpc-and-sql-pushdown-embedders
  - t05-shm-embedding-broker-and-local-models
  - t06-vtb-ai-cli-and-colocated-workers
traces_to:
  - REQ-07
  - NFR-02
tags:
  - docs
  - examples
  - okf
  - embeddings
  - vector
generated:
  by: flow/0.26.0
  at: 2026-11-01T18:38:00Z
verified:
  - by: agent:plan-generator
    at: 2026-11-01T18:38:00Z
    commit: d3d5f8b
sources:
  - id: zensical-config
    resource: zensical.toml
    title: Zensical documentation site navigation and build configuration
  - id: okf-index
    resource: .agents/bundles/knowledge/index.md
    title: OKF v0.2 knowledge base root index
  - id: okf-storage
    resource: .agents/bundles/knowledge/domains/storage/object-storage.md
    title: ObjectStorage domain knowledge chapter
  - id: okf-data
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Database drivers and dialers domain knowledge chapter
  - id: okf-cli
    resource: .agents/bundles/knowledge/domains/framework/cli.md
    title: CLI domain knowledge chapter
  - id: okf-workers
    resource: .agents/bundles/knowledge/domains/server/worker-lanes.md
    title: Worker lanes and supervisor domain knowledge chapter
---

# Task `t07`: Embeddings Documentation, Runnable Example & OKF v0.2 Knowledge Base Sync

<!-- truth: start -->
- **User Documentation & Runnable Example**: `docs/usage/embeddings-and-vectors.md` and `docs/examples/embeddings_app.py` document `Embedder`, `VertexEmbedder`, `GeminiEmbedder`, `GemmaEmbedder`, `AlloyDbEmbedder`, `OracleEmbedder`, `FastEmbedder`, `StaticEmbedder`, `RemoteEmbedder`, `VectorView`, `VectorBatchView`, `vtb ai models`, and `vtb run --with-workers` with high-level Mermaid conceptual diagrams (4–6 nodes) and `pymdownx.tabbed` installation blocks (`=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"`).
- **OKF v0.2 Knowledge Base Sync**: Detailed implementation architecture diagrams, bounded `<!-- truth: start/end -->` blocks, and provenance footnotes are curated into `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md`, [`domains/storage/object-storage.md`](../../../knowledge/domains/storage/object-storage.md)[^okf-storage], [`domains/data/database-drivers-and-dialers.md`](../../../knowledge/domains/data/database-drivers-and-dialers.md)[^okf-data], [`domains/framework/cli.md`](../../../knowledge/domains/framework/cli.md)[^okf-cli], [`domains/server/worker-lanes.md`](../../../knowledge/domains/server/worker-lanes.md)[^okf-workers], and [`.agents/bundles/knowledge/index.md`](../../../knowledge/index.md)[^okf-index].
- **Full Documentation & Review Gate Verification**: `make docs` and `make review-check` pass with zero broken links, zero docstring warnings, and zero privacy or bundle validation errors.
<!-- truth: end -->

---

## 1. Objective & Scope

### 1.1 Goal

Author the user-facing documentation guide, runnable example application, and OKF v0.2 knowledge chapters covering shared-memory vectors, embeddings, active GCP ADC credential refresh, and the `vtb ai` CLI.

### 1.2 In-Scope Files

- `docs/usage/embeddings-and-vectors.md`
- `docs/examples/embeddings_app.py`
- [`zensical.toml`](../../../../../zensical.toml)
- `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md`
- [`.agents/bundles/knowledge/domains/storage/object-storage.md`](../../../knowledge/domains/storage/object-storage.md)
- [`.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`](../../../knowledge/domains/data/database-drivers-and-dialers.md)
- [`.agents/bundles/knowledge/domains/framework/cli.md`](../../../knowledge/domains/framework/cli.md)
- [`.agents/bundles/knowledge/domains/server/worker-lanes.md`](../../../knowledge/domains/server/worker-lanes.md)
- [`.agents/bundles/knowledge/index.md`](../../../knowledge/index.md)

---

## 2. Public Seam & Implementation Specification

### 2.1 User Guide (`docs/usage/embeddings-and-vectors.md`) & Navigation (`zensical.toml`)

1. **Installation Tabs**:
   - Include linked `pymdownx.tabbed` installation blocks (`=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"`) showing both base `vertebra-core` (for Google zero-weight `VertexEmbedder`, `GeminiEmbedder`, `AlloyDbEmbedder`, `OracleEmbedder`, `RemoteEmbedder`, `StaticEmbedder`, and `VectorView` / `VectorBatchView`) and `vertebra-core[embeddings]` (for local `GemmaEmbedder` and `FastEmbedder`).
2. **Conceptual Mermaid Diagram**:
   - Keep the Mermaid diagram in `docs/usage/embeddings-and-vectors.md` high-level and conceptual (4–6 nodes focused on user mental models: `Route / Worker` -> `Embedder` -> `Shared-Memory VectorView / VectorBatchView` -> `Database / VectorCache`).
3. **Sections**:
   - Default Google-first zero-weight embeddings (`Embedder()`, `VertexEmbedder`, `GeminiEmbedder`, `AlloyDbEmbedder`, `OracleEmbedder`).
   - Local `1x` RAM multi-worker embeddings (`GemmaEmbedder`, `FastEmbedder`, `StaticEmbedder`) and `EmbeddingGemma 2` precision guidance (`int8` / `q4` / `bf16` / `fp32`).
   - Zero-copy PEP 3118 `VectorView` and `VectorBatchView` (`memoryview`, `numpy.asarray`, `to_arrow_shm`, `to_pgvector_bytes`, `to_oracle_bytes`, Matryoshka `.truncate()`, `.quantize_int8()`, `.quantize_binary()`).
   - Staging and warming models for Cloud Run without a Hugging Face account (`vtb ai models list|pull|stage|warm`) and running co-located workers (`vtb run app:app --with-workers`).
4. **Register in `zensical.toml`**:
   - Add `usage/embeddings-and-vectors.md` under the `Usage` navigation section in [`zensical.toml`](../../../../../zensical.toml)[^zensical-config].

### 2.2 Runnable Example (`docs/examples/embeddings_app.py`)

- Create a self-contained runnable Vertebra application demonstrating:
  - DI injection (`emb: Inject[Embedder]`) with `StaticEmbedder` / `VertexEmbedder`.
  - Singular `await emb.embed_query(q)` and bulk `await emb.embed_many(docs)`.
  - Zero-copy `memoryview(batch)`, Matryoshka `.truncate(256)`, and off-GIL `emb.similarity_search(q, batch, top_k=5)`.

### 2.3 OKF v0.2 Knowledge Chapters (`.agents/bundles/knowledge/`)

- Create `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md` with detailed implementation architecture diagrams (`VectorSlab` 64-byte header layout, `EmbeddingBroker` token-budgeted length-sorted batching, Protobuf v3 packed-`f32` gRPC wire layout, and `3.14t`/`3.15t` GIL quarantine).
- Update [`domains/storage/object-storage.md`](../../../knowledge/domains/storage/object-storage.md)[^okf-storage], [`domains/data/database-drivers-and-dialers.md`](../../../knowledge/domains/data/database-drivers-and-dialers.md)[^okf-data], [`domains/framework/cli.md`](../../../knowledge/domains/framework/cli.md)[^okf-cli], and [`domains/server/worker-lanes.md`](../../../knowledge/domains/server/worker-lanes.md)[^okf-workers] to document `RefreshingGcpCredentialProvider`, `GcpTokenCache` (`300s` proactive refresh skew + single-flight lock), per-session `applied_token_generation: u64` DuckDB `httpfs` secret rotation, binary `pgvector` / zero-copy `PyBuffer` Oracle 26ai `0xDB` codecs, `vtb ai` (`models` + `mcp`), and co-located `WorkerRunner` execution in `vtb run` / `vtb dev`.
- Register the new chapter in [`.agents/bundles/knowledge/index.md`](../../../knowledge/index.md)[^okf-index].

---

## 3. TDD Verification Plan

### 3.1 Verification Sequence

1. Run `.venv/bin/python -m tools.scripts.review_setup validate` to verify all OKF v0.2 frontmatter, source resources, relative links, and privacy rules.
2. Run `make docs` to verify docstring scanning, CLI doc sync (`generate_cli_docs.py --check`), Zensical build, and postbuild verification.

### 3.2 Verification Commands

```bash
.venv/bin/python -m tools.scripts.review_setup validate
make docs
```

---

## 4. Task Notes

*(Populated during `/flow:implement` execution.)*

---

## Provenance Footnotes

[^zensical-config]: `zensical.toml` — Zensical documentation site navigation and build configuration.
[^okf-index]: `.agents/bundles/knowledge/index.md` — OKF v0.2 knowledge base root index.
[^okf-storage]: `.agents/bundles/knowledge/domains/storage/object-storage.md` — `ObjectStorage` domain knowledge chapter.
[^okf-data]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Database drivers and dialers domain knowledge chapter.
[^okf-cli]: `.agents/bundles/knowledge/domains/framework/cli.md` — CLI domain knowledge chapter.
[^okf-workers]: `.agents/bundles/knowledge/domains/server/worker-lanes.md` — Worker lanes and supervisor domain knowledge chapter.
