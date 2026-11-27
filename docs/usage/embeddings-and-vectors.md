# Vector Embeddings & Shared-Memory Slabs

Vertebra stores embedding vectors in POSIX shared memory with an in-memory cache so worker processes share results without extra copies. It defaults to Google Cloud REST and gRPC embedders and in-database SQL pushdown, with optional local models through `EmbeddingBroker`.

## Installation

Cloud embedders, SQL pushdown adapters, `StaticEmbedder`, and vector views ship in base `vertebra-core`. Install the `[embeddings]` extra only when running local ONNX or EmbeddingGemma models:

```bash
uv add vertebra-core
uv add "vertebra-core[embeddings]"
```

## Embedder Selection

| Class | Default Model | Backend | Extra Required |
| --- | --- | --- | --- |
| `Embedder` | `gemini-embedding-001` (768 dims) | Delegates to `VertexEmbedder` | None |
| `VertexEmbedder` | `gemini-embedding-001` (768 dims) | Vertex AI HTTP/2 REST | None |
| `GeminiEmbedder` | `gemini-embedding-001` (768 dims) | Gemini HTTP/2 gRPC (API key or ADC) | None |
| `PushdownEmbedder` | `gemini-embedding-001` (768 dims) | BigQuery, Spanner, PostgreSQL/AlloyDB, or Oracle SQL | Matching DB extra |
| `StaticEmbedder` | `potion-base-8M` (512 dims) | Memory-mapped static token lookup | None |
| `GemmaEmbedder` | `embeddinggemma-2-270m` (768 dims) | Local broker (`int8`, `q4`, `bf16`, `fp32`) | `[embeddings]` |
| `FastEmbedder` | `BAAI/bge-small-en-v1.5` (384 dims) | Local broker (ONNX Runtime) | `[embeddings]` |
| `RemoteEmbedder` | `gemini-embedding-001` (768 dims) | Remote HTTP/2 gRPC `BatchEmbed` | None |

## Google Cloud & In-Database SQL Pushdown

`Embedder()` defaults to `VertexEmbedder("gemini-embedding-001", dimensions=768)`.

- **`VertexEmbedder`**: Calls Vertex AI over HTTP/2 REST with automatic GCP token caching and `autoTruncate=False` by default. Supports `embed_multimodal()` for `gemini-embedding-2`.
- **`GeminiEmbedder`**: Calls Gemini over HTTP/2 gRPC using an API key or Application Default Credentials, with multimodal support via `embed_multimodal()`.
- **`RemoteEmbedder`**: Calls a remote gRPC embedding service and accepts `id_token_cache` and `audience` for Cloud Run or IAP authentication.

```python
from vertebra.embeddings import (
    Embedder,
    GeminiEmbedder,
    RemoteEmbedder,
    VertexEmbedder,
)
from vertebra.storage import SyncGcpIdTokenCache

default_emb = Embedder()
vertex_emb = VertexEmbedder(
    "gemini-embedding-001",
    project="my-gcp-project",
    location="us-central1",
    dimensions=768,
)
gemini_emb = GeminiEmbedder("gemini-embedding-001")
remote_emb = RemoteEmbedder(
    endpoint="https://embed-svc-xyz.a.run.app",
    id_token_cache=SyncGcpIdTokenCache(),
)
```

To generate embeddings inside your database, pass a sync or async driver to the matching pushdown adapter (BigQuery, Spanner, PostgreSQL/AlloyDB, or Oracle), then wrap it in `PushdownEmbedder`. Wrapping adds shared-memory caching, batch embedding, Arrow output, and in-memory similarity search:

```python
from vertebra.db.adapters.postgres import (
    AsyncPostgreSQLDriver,
    PostgresEmbeddingPushdown,
    SyncPostgreSQLDriver,
)
from vertebra.embeddings import PushdownEmbedder


def create_sql_embedder(
    driver: AsyncPostgreSQLDriver | SyncPostgreSQLDriver,
) -> PushdownEmbedder:
    pushdown = PostgresEmbeddingPushdown(driver, dimensions=768)
    return PushdownEmbedder(pushdown, dimensions=768)
```

- **Search**: Call `await embedder.vector_search("query", table="documents")` to search inside the database, or `embedder.similarity_search(query, candidates)` to search an in-memory batch. Both accept per-call `dimensions` and `normalize` options.
- **Engine SQL**: BigQuery uses `ML.GENERATE_EMBEDDING` or `AI.GENERATE_EMBEDDING`. Spanner uses `ML.PREDICT` or `AI.GENERATE_EMBEDDING` when `use_ai_function=True`. Oracle batches embeddings through `DBMS_VECTOR.UTL_TO_EMBEDDINGS` and rotates credentials from an explicit `token_cache`.
- **Validation & errors**: Pushdown adapters validate dimensions, `top_k`, table and column identifiers, and numeric vector cells. Empty or malformed inference rows raise `EmbeddingError`; other driver errors propagate directly.

## Local Model Broker & EmbeddingGemma

Across multiple HTTP or queue workers, `EmbeddingBroker` loads local model weights once in a shared process, builds length-sorted batches, and writes outputs into shared memory. On free-threaded Python, the broker isolates ONNX Runtime and CTranslate2 in a child process so worker threads stay GIL-free:

```python
from vertebra.embeddings import (
    FastEmbedder,
    GemmaEmbedder,
    StaticEmbedder,
)

gemma_emb = GemmaEmbedder(
    "embeddinggemma-2-270m",
    dimensions=256,
    precision="int8",
)
fast_emb = FastEmbedder("BAAI/bge-small-en-v1.5")
static_emb = StaticEmbedder(dimensions=256)
```

!!! warning "EmbeddingGemma precision"
    EmbeddingGemma activations overflow fp16. `GemmaEmbedder` accepts only int8, q4, bf16, or fp32 and raises `ValueError` for `precision="fp16"`.

Async brokers isolate state per running event loop so concurrent loops do not share handles. For custom local backends, pass an in-process `backend_fn` callable or a `BackendFactory` pointing at an importable callable so the isolated child process can load it on free-threaded Python.

## Vector Views, Truncation & Binary Codecs

`VectorView` (1D) and `VectorBatchView` (2D) implement the Python buffer protocol (for `memoryview` and NumPy arrays), Matryoshka truncation with L2 re-normalization, int8 and binary quantization, and binary database wire encoders:

```python title="docs/examples/embeddings_app.py"
--8<-- "docs/examples/embeddings_app.py:example"
```

## Staging Models & Co-Located Workers

Stage model bundles into GCS, S3, R2, Azure, or OCI so containers do not rely on Hugging Face at runtime. Warm the shared-memory cache on container startup and run co-located workers alongside HTTP ingress:

```bash
vtb ai models list
vtb ai models pull embeddinggemma-2-270m --quant int8
vtb ai models stage embeddinggemma-2-270m --to gs://my-bucket/models/
vtb ai models warm embeddinggemma-2-270m --from gs://my-bucket/models/ --shm
vtb run app:app --with-workers --queues default,embeddings \
    --lane-concurrency default=8,embeddings=2
```
