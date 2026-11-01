"""Google-first zero-weight and shared-memory local vector embeddings for Vertebra."""

from __future__ import annotations

from vertebra.embeddings._base import (
    Embedder,
    EmbeddingTask,
    MultimodalPart,
    SimilarityMatch,
    VectorBatchView,
    VectorCache,
    VectorView,
)
from vertebra.embeddings._broker import (
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

__all__ = (
    "MODEL_CATALOG",
    "AlloyDbEmbedder",
    "BackendInferenceCallable",
    "Embedder",
    "EmbeddingBroker",
    "EmbeddingBrokerConfig",
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
    "StaticEmbedder",
    "VectorBatchView",
    "VectorCache",
    "VectorView",
    "VertexEmbedder",
    "create_embedding_grpc_service",
    "get_model_spec",
    "list_model_specs",
)
