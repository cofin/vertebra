"""Google-first zero-weight and shared-memory local vector embeddings for Vertebra."""

from __future__ import annotations

from vertebra.embeddings._base import (
    Embedder,
    EmbeddingError,
    EmbeddingTask,
    MultimodalPart,
    SimilarityMatch,
    VectorBatchView,
    VectorCache,
    VectorView,
)
from vertebra.embeddings._bigquery import BigQueryEmbedder
from vertebra.embeddings._broker import (
    BackendFactory,
    BackendInferenceCallable,
    EmbeddingBroker,
    EmbeddingBrokerConfig,
)
from vertebra.embeddings._google import (
    AlloyDbEmbedder,
    GeminiEmbedder,
    GrpcTransportCallable,
    OracleEmbedder,
    VertexEmbedder,
)
from vertebra.embeddings._local import (
    FastEmbedder,
    GemmaEmbedder,
    StaticEmbedder,
)
from vertebra.embeddings._models import (
    MODEL_CATALOG,
    ModelSpec,
    get_model_spec,
    list_model_specs,
)
from vertebra.embeddings._remote import (
    RemoteEmbedder,
    RemoteEmbedRequest,
    RemoteEmbedResponse,
    create_embedding_grpc_service,
)
from vertebra.embeddings._spanner import SpannerEmbedder

__all__ = (
    "MODEL_CATALOG",
    "AlloyDbEmbedder",
    "BackendFactory",
    "BackendInferenceCallable",
    "BigQueryEmbedder",
    "Embedder",
    "EmbeddingBroker",
    "EmbeddingBrokerConfig",
    "EmbeddingError",
    "EmbeddingTask",
    "FastEmbedder",
    "GeminiEmbedder",
    "GemmaEmbedder",
    "GrpcTransportCallable",
    "ModelSpec",
    "MultimodalPart",
    "OracleEmbedder",
    "RemoteEmbedRequest",
    "RemoteEmbedResponse",
    "RemoteEmbedder",
    "SimilarityMatch",
    "SpannerEmbedder",
    "StaticEmbedder",
    "VectorBatchView",
    "VectorCache",
    "VectorView",
    "VertexEmbedder",
    "create_embedding_grpc_service",
    "get_model_spec",
    "list_model_specs",
)
