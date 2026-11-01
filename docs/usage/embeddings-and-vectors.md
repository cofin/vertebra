# Vector Embeddings & Shared-Memory Slabs

`vertebra.embeddings` provides shared-memory vector embeddings (`VectorView`, `VectorBatchView`) backed by a POSIX shared-memory `VectorSlab` and a set-associative seqlock `VectorCache`. It defaults to zero-weight Google HTTP/2 gRPC (`VertexEmbedder`, `GeminiEmbedder`) and in-database SQL pushdown (`AlloyDbEmbedder`, `OracleEmbedder`), while supporting `1x` RAM local models (`GemmaEmbedder`, `FastEmbedder`, `StaticEmbedder`) through `EmbeddingBroker`.

```mermaid
flowchart LR
    Route["Route / Worker"] --> Embedder["Embedder Facade"]
    Embedder --> Cache["SHM VectorCache"]
    Embedder --> Backend["Cloud gRPC / DB SQL / Local Broker"]
    Backend --> View["PEP 3118 VectorView / VectorBatchView"]
    View --> Store["pgvector / Oracle 26ai / Arrow IPC"]
```

## Installation

Zero-weight cloud gRPC (`VertexEmbedder`, `GeminiEmbedder`, `RemoteEmbedder`), database SQL pushdown (`AlloyDbEmbedder`, `OracleEmbedder`), pure-Rust `StaticEmbedder`, and `VectorView` / `VectorBatchView` ship in base `vertebra-core`. Install the `[embeddings]` extra only when running local ONNX / `EmbeddingGemma` weights:

=== "uv"

    ```bash
    uv add vertebra-core
    uv add "vertebra-core[embeddings]"
    ```

=== "pip"

    ```bash
    pip install vertebra-core
    pip install "vertebra-core[embeddings]"
    ```

=== "pdm"

    ```bash
    pdm add vertebra-core
    pdm add "vertebra-core[embeddings]"
    ```

=== "poetry"

    ```bash
    poetry add vertebra-core
    poetry add "vertebra-core[embeddings]"
    ```

## Embedder Selection Matrix

| Class | Default Model | Transport / Engine | Extra Required |
| --- | --- | --- | --- |
| `Embedder` | `gemini-embedding-001` (`768` dims) | Delegates to `VertexEmbedder` by default | None |
| `VertexEmbedder` | `gemini-embedding-001` (`768` dims) | Rust HTTP/2 gRPC + `RefreshingGcpCredentialProvider` | None |
| `GeminiEmbedder` | `gemini-embedding-001` (`768` dims) | Rust HTTP/2 gRPC (`x-goog-api-key` or ADC) | None |
| `AlloyDbEmbedder` | `gemini-embedding-001` (`768` dims) | In-DB `embedding()` + binary `pgvector` | None |
| `OracleEmbedder` | `gemini-embedding-001` (`768` dims) | In-DB `DBMS_VECTOR.UTL_TO_EMBEDDING()` + `0xDB` wire | None |
| `StaticEmbedder` | `potion-base-8M` (`512` dims) | Pure-Rust `mmap` token lookup (`<50 µs`) | None |
| `GemmaEmbedder` | `embeddinggemma-2-270m` (`768` dims) | `1x` RAM `EmbeddingBroker` (`int8` / `q4` / `bf16` / `fp32`) | `[embeddings]` |
| `FastEmbedder` | `BAAI/bge-small-en-v1.5` (`384` dims) | `1x` RAM `EmbeddingBroker` (ONNX Runtime) | `[embeddings]` |
| `RemoteEmbedder` | `embeddinggemma-2-270m` (`768` dims) | Rust HTTP/2 gRPC `BatchEmbed` | None |

## Google Cloud & In-Database SQL Pushdown

`Embedder()` defaults to `VertexEmbedder("gemini-embedding-001", dimensions=768)` and decodes Protobuf v3 packed-`f32` fields directly into `VectorSlab` off-GIL:

```python
from vertebra.embeddings import (
    AlloyDbEmbedder,
    Embedder,
    GeminiEmbedder,
    OracleEmbedder,
    VertexEmbedder,
)

default_emb = Embedder()
vertex_emb = VertexEmbedder(
    "gemini-embedding-001",
    project="my-gcp-project",
    location="us-central1",
    dimensions=768,
)
gemini_emb = GeminiEmbedder("gemini-embedding-001")
alloy_emb = AlloyDbEmbedder(model="gemini-embedding-001")
oracle_emb = OracleEmbedder(model="gemini-embedding-001")
```

## Local `1x` RAM Broker & `EmbeddingGemma`

When running multiple HTTP or background worker processes, `EmbeddingBroker` loads model weights **once (`1x` RAM)**, forms token-budgeted length-sorted batches (`max_batch_tokens=8192`), and writes outputs into `VectorSlab`. On free-threaded Python `3.14t`/`3.15t`, `EmbeddingBroker` automatically isolates `onnxruntime` / `ctranslate2` in a dedicated child process so HTTP workers keep `sys._is_gil_enabled() == False`:

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

!!! warning "`EmbeddingGemma` Precision"
    `EmbeddingGemma` activations overflow IEEE 754 `fp16`. `GemmaEmbedder` enforces `precision` in `{"int8", "q4", "bf16", "fp32"}` and raises `ValueError` if `precision="fp16"` is requested.

## Zero-Copy `VectorView`, Matryoshka Truncation & Binary Wire Codecs

`VectorView` (1D) and `VectorBatchView` (2D) implement PEP 3118 (`memoryview`, `np.asarray`), SIMD Matryoshka `.truncate(dims)` with L2 re-normalization, scalar `.quantize_int8()`, bit-packed `.quantize_binary()`, and binary database wire encoders:

```python title="docs/examples/embeddings_app.py"
--8<-- "docs/examples/embeddings_app.py:example"
```

## Staging Models with `vtb ai models` & Co-Located Workers

Stage model bundles into GCS, S3, R2, Azure, or OCI without a Hugging Face account at runtime, warm the shared-memory cache on container startup, and run co-located background workers alongside HTTP ingress:

```bash
vtb ai models list
vtb ai models pull embeddinggemma-2-270m --quant int8
vtb ai models stage embeddinggemma-2-270m --to gs://my-bucket/models/
vtb ai models warm embeddinggemma-2-270m --from gs://my-bucket/models/ --shm
vtb run app:app --with-workers --queues default,embeddings \
    --lane-concurrency default=8,embeddings=2
```
